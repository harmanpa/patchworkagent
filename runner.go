package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
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

	"github.com/pkg/errors"
)

type Artefact struct {
	name        string `json:"name"`
	contentType string `json:"contentType"`
	uri         string `json:"uri"`
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

type PubSubPayload struct {
	Message PubSubMessage `json:"message"`
}

type PubSubMessage struct {
	MessageId   string `json:"messageId"`
	PublishTime string `json:"publishTime"`
	Data        string `json:"data"`
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
					// Try to get a payload
					var calc CalculationPayload
					err = json.Unmarshal(StringToBytes(payload), &calc)
					if err != nil {
						// It might be in the Google PubSub format
						var pubsub PubSubPayload
						err = json.Unmarshal(StringToBytes(payload), &pubsub)
						if err == nil {
							data, err := base64.StdEncoding.DecodeString(pubsub.Message.Data)
							if err == nil {
								err = json.Unmarshal(data, &calc)
								if err == nil {
									err = RunCalculation(command, calc.Host, calc.Token, calc.Id, dir, timeout)
								}
							}
						}
					} else {
						err = RunCalculation(command, calc.Host, calc.Token, calc.Id, dir, timeout)
					}
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
	calcContext, err, abort := GetContext(host, token, calculation)
	if abort {
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// Write the inputs to files in the working directory
	log.Println("Expanding inputs of calculation " + calculation)
	err = ExpandContext(dirpath, token, calcContext)
	if err != nil {
		return errors.WithStack(err)
	}

	// Get a timestamp before running the calculation
	t := time.Now()

	// Create a new context and add a timeout to it. Deferred rather than
	// cancelled at each exit: the early return below - the server refusing the
	// first log - left it uncancelled, leaking a context and its timer per
	// calculation for the life of an agent serving over HTTP.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()

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
	// Extend the environment rather than replace it. Replacing it leaves the
	// command with no PATH and, in an image whose runtime is set up by its
	// entrypoint - a conda environment, a toolchain activation - none of that
	// setup either, so the command cannot find the very tools the image exists
	// to provide.
	cmd.Env = append(os.Environ(),
		"HOST="+host,
		"TOKEN="+token,
		// The calculation the command is running for, so that a long task can
		// report its own progress to /api/calculations/logs/<id>. Without it the
		// only progress the server sees is the one this agent sends before the
		// command starts, and a run of any length looks stalled.
		"CALCULATION="+calculation)

	// Capture stdout/stderr
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	// Notify the server that we are now Running
	err = SendLogs(host, token, calculation, "", 0.0)
	if err != nil {
		return errors.WithStack(err)
	}

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
		return errors.WithStack(err)
	}

	// Send the data to the server
	log.Println("Uploading results of calculation " + calculation)
	err = SendResult(host, token, calculation, response)
	log.Println("Completing calculation " + calculation)
	return errors.WithStack(err)
}

func GetContext(host string, token string, calculation string) (CalculationContext, error, bool) {
	var dat CalculationContext
	var abort bool
	abort = false
	req, err := http.NewRequest("GET", host+"/api/calculations/remote/"+calculation, nil)
	if err != nil {
		return dat, errors.WithStack(err), abort
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	// Check the error before the response: a request that failed to go out at
	// all - no route, TLS refused, host not resolving - returns a nil response,
	// and reading its status panics instead of reporting what went wrong.
	if err != nil {
		return dat, errors.WithStack(err), abort
	}
	defer resp.Body.Close()
	// HTTP code to indicate we already ran the calculation
	if resp.StatusCode == 208 {
		abort = true
		return dat, nil, abort
	}
	if resp.StatusCode != 200 {
		return dat, errors.New(resp.Status), abort
	}
	err = json.Unmarshal(StreamToBytes(resp.Body), &dat)
	return dat, errors.WithStack(err), abort
}

func ExpandContext(dirpath string, token string, context CalculationContext) error {
	for name, content := range context.Inputs {
		err := ExpandContextFile(dirpath, token, name, content)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func ExpandContextFile(dirpath string, token string, name string, content interface{}) error {
	isArtefact, err := HandleAsArtefact(dirpath, token, name, content)
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

func StringsToJson(strs []string) string {
	out := make([]string, len(strs))
	for i, s := range strs {
		sjson, err := json.Marshal(s)
		if err != nil {
			out[i] = "\"\""
		} else {
			out[i] = string(sjson)
		}
	}
	return "[" + strings.Join(out, ", ") + "]"
}

func PackageResult(dirpath string, since time.Time, stdout string, stderr string) (string, error) {
	response := "{\n"
	response += "\t\"logs\": " + StringsToJson(TrimAndSplit(stdout)) + ",\n"
	response += "\t\"errors\": " + StringsToJson(TrimAndSplit(stderr)) + ",\n"
	response += "\t\"outputs\": {\n"
	files, err := GetChangedFiles(dirpath, since)
	if err != nil {
		return response, errors.WithStack(err)
	}
	first := true
	for _, file := range files {
		var err error
		filedata, err := HandleOutputFile(file)
		if err != nil {
			return response, errors.WithStack(err)
		}
		if first {
			first = false
		} else {
			response += ",\n"
		}
		response += "\t\t\"" + filepath.Base(file) + "\": " + filedata
	}
	response += "\n\t}\n}"
	return response, nil
}

func HandleOutputFile(file string) (string, error) {
	log.Println("Reading output file " + file)
	if strings.HasSuffix(file, ".json") {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", errors.WithStack(err)
		}
		return string(data), nil
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
			changed = append(changed, filepath.Join(dirpath, file.Name()))
		}
	}
	return changed, errors.WithStack(err)
}

func SendLogs(host string, token string, calculation string, log string, progress float32) error {
	req, err := http.NewRequest("POST", host+"/api/calculations/logs/"+calculation+"?progress="+fmt.Sprintf("%f", progress), strings.NewReader(log))
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "text/plain")
	_, err = http.DefaultClient.Do(req)
	return errors.WithStack(err)
}

func SendResult(host string, token string, calculation string, response string) error {
	req, err := http.NewRequest("POST",
		host+"/api/calculations/remote/"+calculation,
		strings.NewReader(response))
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return nil
}

func MakeArtefact(path string) (string, error) {
	log.Println("Converting file to Artefact")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	name := filepath.Base(path)
	contentType := http.DetectContentType(data)
	uri := "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(data)
	log.Println("Detected content-type of " + contentType)
	uribytes, err := json.Marshal(uri)
	if err != nil {
		return "", err
	}
	return "{\"name\": \"" + name + "\", \"contentType\": \"" + contentType + "\", \"uri\": " + string(uribytes) + "}", nil
}

func HandleAsArtefact(dirpath string, token string, name string, content interface{}) (bool, error) {
	if content != nil {
		toexpand, ok := content.(map[string]interface{})
		if !ok {
			return false, nil
		}
		if toexpand["name"] != nil && toexpand["uri"] != nil && toexpand["contentType"] != nil {
			err := ReadArtefact(dirpath, token, name, Artefact{
				name:        toexpand["name"].(string),
				contentType: toexpand["contentType"].(string),
				uri:         toexpand["uri"].(string),
			})
			return true, errors.WithStack(err)
		}
	}
	return false, nil
}

// InputPath is where an artefact input is written: named after the input, with
// the uploaded file's extension, so a command can find it without being told.
func InputPath(dirpath string, name string, artefact Artefact) string {
	extension := ""
	if dot := strings.LastIndex(artefact.name, "."); dot > -1 {
		extension = artefact.name[dot:]
	}
	return dirpath + "/" + name + extension
}

// ReadArtefact writes an artefact input to a file.
//
// The server sends small ones inline as a data: URI and large ones as a URL to
// fetch, because a base64 data: URI has to be built whole in memory at both ends
// of a request that cannot stream or resume. A fetched one is streamed to disk
// for the same reason, so the size of an input is bounded by the disk rather
// than by this process.
func ReadArtefact(dirpath string, token string, name string, artefact Artefact) error {
	path := InputPath(dirpath, name, artefact)
	if strings.HasPrefix(artefact.uri, "data:") {
		b64 := strings.SplitN(artefact.uri, ",", 2)[1]
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return errors.WithStack(err)
		}
		log.Println("Writing input file " + path)
		return errors.WithStack(os.WriteFile(path, raw, os.ModePerm))
	}
	if strings.HasPrefix(artefact.uri, "http://") || strings.HasPrefix(artefact.uri, "https://") {
		log.Println("Downloading input file " + path + " from " + artefact.uri)
		return errors.WithStack(DownloadArtefact(artefact.uri, token, path))
	}
	// A blob: reference means nothing outside the server, and anything else is
	// not something this agent can resolve either.
	return errors.New("Cannot read artefact " + artefact.name + " from URI of unsupported form")
}

// DownloadArtefact streams a URL to a file, authenticating as the task.
func DownloadArtefact(url string, token string, path string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(token) > 0 {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("Failed to download " + url + ": " + resp.Status)
	}
	out, err := os.Create(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer out.Close()
	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Println("Downloaded " + strconv.FormatInt(written, 10) + " bytes to " + path)
	return nil
}
