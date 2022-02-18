package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Artefact struct {
	Name        string `json:"name"`
	ContentType string `json:"contentType"`
	URI         string `json:"uri"`
}

type CalculationId struct {
	DocumentType string `json:"documentType"`
	Type         string `json:"type"`
	Id           string `json:"id"`
	Version      string `json:"version"`
	Path         string `json:"path"`
}

type CalculationPayload struct {
	Id    string `json:"id"`
	Host  string `json:"host"`
	Token string `json:"token"`
}

type CalculationContext struct {
	Id           CalculationId          `json:"id"`
	Owner        string                 `json:"owner"`
	Inputs       map[string]interface{} `json:"inputs"`
	FailedInputs map[string]string      `json:"failedInputs"`
}

type CalculationResponse struct {
	Outputs map[string]interface{} `json:"outputs"`
	Logs    []string               `json:"logs"`
	Errors  []string               `json:"errors"`
}

func main() {
	log.SetFlags(0)
	log.Println("Patchwork Calculation Agent")
	// Get the current directory
	dirpath, err := os.Getwd()
	if err != nil {
		log.Fatal(fmt.Sprintf("%+v\n", err))
	}
	log.Println("Running in " + dirpath)
	// Define the command line flags
	cmdPtr := flag.String("c", "", "Command to execute")
	hostPtr := flag.String("h", "", "Host of calling app")
	tokenPtr := flag.String("t", "", "Security token")
	concurrencyPtr := flag.String("concurrency", "4", "Concurrency if http server")
	timeoutPtr := flag.String("timeout", "3600", "Timeout in s")
	flag.Parse()
	log.Println("Calculation command is " + *cmdPtr)
	if len(*cmdPtr) == 0 {
		log.Fatal("No command provided")
	}
	timeout, err := strconv.Atoi(*timeoutPtr)
	if err != nil {
		timeout = 3600
	}
	args := flag.Args()
	if len(args) > 0 {
		// The calculation has been passed via the CLI
		//if len(*tokenPtr) == 0 {
		//	log.Fatal("No token provided")
		//}
		if len(*hostPtr) == 0 {
			log.Fatal("No host provided")
		}
		err = RunCalculation(*cmdPtr, *hostPtr, *tokenPtr, args[0], dirpath, timeout)
		if err != nil {
			log.Fatal(fmt.Sprintf("%+v\n", err))
		}
	} else {
		// Get the concurrency
		concurrency, err := strconv.Atoi(*concurrencyPtr)
		if err != nil {
			concurrency = 4
		}
		// The calculation will be passed via HTTP
		err = Server(*cmdPtr, *hostPtr, *tokenPtr, dirpath, concurrency, timeout)
		if err != nil {
			log.Fatal(fmt.Sprintf("%+v\n", err))
		}
	}
}

func Server(command string, host string, token string, dirpath string, concurrency int, timeout int) error {
	http.HandleFunc("/", limitNumClients(func(writer http.ResponseWriter, request *http.Request) {
		if "POST" == strings.ToUpper(request.Method) {
			// TODO: This should handle some different structures: Google Pubsub, or just a string etc
			// RunCalculation()
			dir, err := ioutil.TempDir(dirpath, "calc")
			if err != nil {
				log.Println(fmt.Sprintf("%+v\n", err))
				writer.WriteHeader(500)
			} else {
				payload := StreamToString(request.Body)
				if strings.HasPrefix(payload, "{") {
					var calc CalculationPayload
					json.Unmarshal(StringToBytes(payload), &calc)
					err = RunCalculation(command, calc.Host, calc.Token, calc.Id, dir, timeout)
				} else {
					err = RunCalculation(command, host, token, payload, dir, timeout)
				}
				os.RemoveAll(dir)
				if err != nil {
					log.Println(fmt.Sprintf("%+v\n", err))
					writer.WriteHeader(500)
				} else {
					writer.WriteHeader(200)
				}
			}
		} else {
			writer.WriteHeader(404)
		}
	}, concurrency))
	log.Println("Starting server on port 8080")
	err := http.ListenAndServe(":8080", nil)
	return errors.WithStack(err)
}

// limitNumClients is HTTP handling middleware that ensures no more than
// maxClients requests are passed concurrently to the given handler f.
func limitNumClients(f http.HandlerFunc, maxClients int) http.HandlerFunc {
	sema := make(chan struct{}, maxClients)

	return func(w http.ResponseWriter, req *http.Request) {
		sema <- struct{}{}
		defer func() { <-sema }()
		f(w, req)
	}
}

func RunCalculation(command string, host string, token string, calculation string, dirpath string, timeout int) error {
	log.Println("Preparing calculation " + calculation)
	// Remove trailing slash from URL
	host = strings.TrimSuffix(host, "/")

	// Get all the data from the server about this calculation
	log.Println("Fetching inputs of calculation " + calculation)
	calcContext, err := GetContext(host, token, calculation)
	if err != nil {
		return errors.WithStack(err)
	}

	// Write the inputs to files in the working directory
	log.Println("Expanding inputs of calculation " + calculation)
	err = ExpandContext(dirpath, calcContext)
	if err != nil {
		return errors.WithStack(err)
	}

	// Get a timestamp before running the calculation
	t := time.Now()

	// Create a new context and add a timeout to it
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))

	// Make a Cmd object
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/c",
			strings.TrimSuffix(strings.TrimPrefix(command, "\""), "\""))
	} else {
		cmd = exec.CommandContext(ctx, "bash", "-c",
			strings.TrimSuffix(strings.TrimPrefix(command, "\""), "\""))
	}
	cmd.Dir = dirpath

	// Capture stdout/stderr
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	// Run the command
	log.Println("Running calculation " + calculation)
	err = cmd.Run()
	if err != nil {
		stderrBuf.WriteString(err.Error())
	}

	// We want to check the context error to see if the timeout was executed.
	// The error returned by cmd.Output() will be OS specific based on what
	// happens when a process is killed.
	if ctx.Err() == context.DeadlineExceeded {
		stderrBuf.WriteString("Command timed out")
	}
	outStr, errStr := string(stdoutBuf.Bytes()), string(stderrBuf.Bytes())

	// Find all files changed during the task and package them to return to server
	log.Println("Packaging results of calculation " + calculation)
	response, err := PackageResult(dirpath, t, outStr, errStr)
	if err != nil {
		// Cleanup
		cancel()
		return errors.WithStack(err)
	}

	// Send the data to the server
	log.Println("Uploading results of calculation " + calculation)
	err = SendResult(host, token, calculation, response)
	log.Println("Completing calculation " + calculation)
	// Cleanup
	cancel()
	return errors.WithStack(err)
}

func GetContext(host string, token string, calculation string) (CalculationContext, error) {
	var dat CalculationContext
	req, err := http.NewRequest("GET", host+"/api/calculations/remote/"+calculation, nil)
	if err != nil {
		return dat, errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return dat, errors.WithStack(err)
	}
	if resp.StatusCode != 200 {
		return dat, errors.New(resp.Status)
	}
	err = json.Unmarshal(StreamToBytes(resp.Body), &dat)
	return dat, errors.WithStack(err)
}

func ExpandContext(dirpath string, context CalculationContext) error {
	for name, content := range context.Inputs {
		err := ExpandContextFile(dirpath, name, content)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func ExpandContextFile(dirpath string, name string, content interface{}) error {
	isArtefact, err := HandleAsArtefact(dirpath, name, content)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isArtefact && content != nil {
		raw, err := json.Marshal(content)
		if err != nil {
			return errors.WithStack(err)
		}
		log.Println("Writing input file " + dirpath + "/" + name + ".json")
		err = os.WriteFile(dirpath+"/"+name+".json", raw, os.ModePerm)
		return errors.WithStack(err)
	}
	return nil
}

func StreamToBytes(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}

func StringToBytes(s string) []byte {
	return StreamToBytes(strings.NewReader(s))
}

func StreamToString(stream io.Reader) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.String()
}

func TrimAndSplit(str string) []string {
	out := make([]string, 0)
	str = strings.Trim(str, " \t\r\n")
	if len(str) > 0 {
		out = strings.Split(str, "\n")
	}
	return out
}

func PackageResult(dirpath string, since time.Time, stdout string, stderr string) (CalculationResponse, error) {
	response := CalculationResponse{
		Outputs: make(map[string]interface{}),
		Logs:    TrimAndSplit(stdout),
		Errors:  TrimAndSplit(stderr),
	}
	files, err := GetChangedFiles(dirpath, since)
	if err != nil {
		return response, errors.WithStack(err)
	}
	for _, file := range files {
		var err error
		response.Outputs[filepath.Base(file)], err = HandleOutputFile(file)
		if err != nil {
			return response, errors.WithStack(err)
		}
	}
	return response, nil
}

func HandleOutputFile(file string) (interface{}, error) {
	log.Println("Reading output file " + file)
	if strings.HasSuffix(file, ".json") {
		data, err := os.ReadFile(file)
		var out interface{}
		if err != nil {
			return out, errors.WithStack(err)
		}
		err = json.Unmarshal(data, &out)
		return out, errors.WithStack(err)
	} else {
		artefact, err := MakeArtefact(file)
		return artefact, errors.WithStack(err)
	}
}

func GetChangedFiles(dirpath string, since time.Time) ([]string, error) {
	log.Println("Looking for files that have changed since " + since.Format(time.RFC3339))
	changed := make([]string, 0)
	files, err := ioutil.ReadDir(dirpath)
	if err != nil {
		return changed, errors.WithStack(err)
	}
	for _, file := range files {
		log.Println("Checking file " + file.Name() + " changed " + file.ModTime().Format(time.RFC3339))
		if !file.IsDir() && file.ModTime().After(since) {
			log.Println("Including file " + file.Name())
			changed = append(changed, file.Name())
		}
	}
	return changed, errors.WithStack(err)
}

func SendLogs(host string, token string, calculation string, log string) error {
	req, err := http.NewRequest("POST", host+"/api/calculations/logs/"+calculation, strings.NewReader(log))
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "text/plain")
	_, err = http.DefaultClient.Do(req)
	return errors.WithStack(err)
}

func SendResult(host string, token string, calculation string, response CalculationResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return errors.WithStack(err)
	}
	req, err := http.NewRequest("POST", host+"/api/calculations/remote/"+calculation, bytes.NewReader(data))
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return errors.WithStack(err)
}

func MakeArtefact(path string) (Artefact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Artefact{}, err
	}
	contentType := http.DetectContentType(data)
	return Artefact{
		Name:        filepath.Base(path),
		ContentType: contentType,
		URI:         "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(data),
	}, nil
}

func HandleAsArtefact(dirpath string, name string, content interface{}) (bool, error) {
	if content != nil {
		toexpand := content.(map[string]interface{})
		if toexpand["name"] != nil && toexpand["uri"] != nil && toexpand["contentType"] != nil {
			err := ReadArtefact(dirpath, name, Artefact{
				Name:        toexpand["name"].(string),
				ContentType: toexpand["contentType"].(string),
				URI:         toexpand["uri"].(string),
			})
			return true, errors.WithStack(err)
		}
	}
	return false, nil
}

func ReadArtefact(dirpath string, name string, artefact Artefact) error {
	if !strings.HasPrefix(artefact.URI, "data:") {
		return errors.New("Not a data URI")
	}
	b64 := strings.SplitN(artefact.URI, ",", 2)[1]
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return errors.WithStack(err)
	}
	extension := artefact.Name[strings.LastIndex(artefact.Name, ".")+1:]
	log.Println("Writing input file " + dirpath + "/" + name + "." + extension)
	err = os.WriteFile(dirpath+"/"+name+"."+extension, raw, os.ModePerm)
	return errors.WithStack(err)
}
