package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
		log.Fatal(err)
	}
	log.Println("Running in " + dirpath)
	// Define the command line flags
	cmdPtr := flag.String("c", "", "Command to execute")
	hostPtr := flag.String("h", "", "Host of calling app")
	tokenPtr := flag.String("t", "", "Security token")
	flag.Parse()
	log.Println("Calculation command is " + *cmdPtr)
	if len(*cmdPtr) == 0 {
		log.Fatal("No command provided")
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
		err = RunCalculation(*cmdPtr, *hostPtr, *tokenPtr, args[0], dirpath)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		// The calculation will be passed via HTTP
		err = Server(*cmdPtr, *hostPtr, *tokenPtr, dirpath)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Server(command string, host string, token string, dirpath string) error {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if "POST" == strings.ToUpper(request.Method) {
			// TODO: This should handle some different structures: Google Pubsub, or just a string etc
			// RunCalculation()
			payload := StreamToString(request.Body)
			var err error
			if strings.HasPrefix(payload, "{") {
				var calc CalculationPayload
				json.Unmarshal(StringToBytes(payload), &calc)
				err = RunCalculation(command, calc.Host, calc.Token, calc.Id, dirpath)
			} else {
				err = RunCalculation(command, host, token, payload, dirpath)
			}
			if err != nil {
				log.Println(err)
				writer.WriteHeader(500)
			} else {
				writer.WriteHeader(200)
			}
		} else {
			writer.WriteHeader(404)
		}
	})
	log.Println("Starting server on port 8080")
	err := http.ListenAndServe(":8080", nil)
	return errors.WithStack(err)
}

func RunCalculation(command string, host string, token string, calculation string, dirpath string) error {
	log.Println("Preparing calculation " + calculation)
	// Get all the data from the server about this calculation
	context, err := GetContext(host, token, calculation)
	if err != nil {
		return errors.WithStack(err)
	}

	// Write the inputs to files in the working directory
	err = ExpandContext(dirpath, context)
	if err != nil {
		return errors.WithStack(err)
	}

	// Get a timestamp before running the calculation
	t := time.Now()

	// Make a Cmd object
	commandArgs := strings.Split(command, " ")
	cmd := exec.Cmd{Path: commandArgs[0], Args: commandArgs[1:]}

	// Capture stdout/stderr
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	// Run the command
	err = cmd.Run()
	if err != nil {
		stderrBuf.WriteString(err.Error())
	}
	outStr, errStr := string(stdoutBuf.Bytes()), string(stderrBuf.Bytes())

	// Find all files changed during the task and package them to return to server
	response, err := PackageResult(dirpath, t, outStr, errStr)
	if err != nil {
		return errors.WithStack(err)
	}

	// Send the data to the server
	err = SendResult(host, token, calculation, response)
	return errors.WithStack(err)
}

func GetContext(host string, token string, calculation string) (CalculationContext, error) {
	resp, err := http.Get(host + "/api/calculations/remote/" + calculation)
	var dat CalculationContext
	if err != nil {
		return dat, errors.WithStack(err)
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
	if !isArtefact {
		raw, err := json.Marshal(content)
		if err != nil {
			return errors.WithStack(err)
		}
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

func PackageResult(dirpath string, since time.Time, stdout string, stderr string) (CalculationResponse, error) {
	response := CalculationResponse{
		Outputs: make(map[string]interface{}),
		Logs:    strings.Split(stdout, "\n"),
		Errors:  strings.Split(stderr, "\n"),
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
	changed := make([]string, 0)
	files, err := ioutil.ReadDir("/tmp/")
	if err != nil {
		return changed, errors.WithStack(err)
	}
	for _, file := range files {
		if file.ModTime().After(since) {
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
	req.Header.Add("Authorization", "Bearer "+token)
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
	req.Header.Add("Authorization", "Bearer "+token)
	_, err = http.DefaultClient.Do(req)
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
	toexpand := content.(map[string]interface{})
	if toexpand["name"] != nil && toexpand["uri"] != nil && toexpand["contentType"] != nil {
		err := ReadArtefact(dirpath, name, Artefact{
			Name:        toexpand["name"].(string),
			ContentType: toexpand["contentType"].(string),
			URI:         toexpand["uri"].(string),
		})
		if err != nil {
			return false, errors.WithStack(err)
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
	extension := name[strings.LastIndex(artefact.Name, ".")+1:]
	err = os.WriteFile(dirpath+"/"+name+"."+extension, raw, os.ModePerm)
	return errors.WithStack(err)
}
