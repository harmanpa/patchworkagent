package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
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
	log.Println("Patchwork Calculation Agent")
	// Get the current directory
	dirpath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	log.Println("Running in " + dirpath)
	// Define the command line flags
	cmdPtr := flag.String("c", "", "Command to execute")
	hostPtr := flag.String("h", "", "Host of calling app")
	tokenPtr := flag.String("t", "", "Security token")
	flag.Parse()
	log.Println("Calculation command is " + *cmdPtr)
	if len(*cmdPtr) == 0 {
		panic("No command provided")
	}
	args := flag.Args()
	if len(args) > 0 {
		// The calculation has been passed via the CLI
		//if len(*tokenPtr) == 0 {
		//	panic("No token provided")
		//}
		if len(*hostPtr) == 0 {
			panic("No host provided")
		}
		RunCalculation(*cmdPtr, *hostPtr, *tokenPtr, args[0], dirpath)
	} else {
		// The calculation will be passed via HTTP
		Server(*cmdPtr, *hostPtr, *tokenPtr, dirpath)
	}
}

func Server(command string, host string, token string, dirpath string) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if "POST" == strings.ToUpper(request.Method) {
			// TODO: This should handle some different structures: Google Pubsub, or just a string etc
			// RunCalculation()
			payload := StreamToString(request.Body)
			if strings.HasPrefix(payload, "{") {
				var calc CalculationPayload
				json.Unmarshal(StringToBytes(payload), &calc)
				RunCalculation(command, calc.Host, calc.Token, calc.Id, dirpath)
			} else {
				RunCalculation(command, host, token, payload, dirpath)
			}
			writer.WriteHeader(200)
		} else {
			writer.WriteHeader(404)
		}
	})
	log.Println("Starting server on port 8080")
	http.ListenAndServe(":8080", nil)
}

func RunCalculation(command string, host string, token string, calculation string, dirpath string) {
	log.Println("Preparing calculation " + calculation)
	// Get all the data from the server about this calculation
	context := GetContext(host, token, calculation)

	// Write the inputs to files in the working directory
	ExpandContext(dirpath, context)

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
	err := cmd.Run()
	if err != nil {
		stderrBuf.WriteString(err.Error())
	}
	outStr, errStr := string(stdoutBuf.Bytes()), string(stderrBuf.Bytes())

	// Find all files changed during the task and package them to return to server
	response := PackageResult(dirpath, t, outStr, errStr)

	// Send the data to the server
	SendResult(host, token, calculation, response)
}

func GetContext(host string, token string, calculation string) CalculationContext {
	resp, err := http.Get(host + "/api/calculations/remote/" + calculation)
	if err != nil {
		panic(err)
	}
	var dat CalculationContext
	err = json.Unmarshal(StreamToBytes(resp.Body), &dat)
	if err != nil {
		panic(err)
	}
	return dat
}

func ExpandContext(dirpath string, context CalculationContext) {
	for name, content := range context.Inputs {
		ExpandContextFile(dirpath, name, content)
	}
}

func ExpandContextFile(dirpath string, name string, content interface{}) {
	if !HandleAsArtefact(dirpath, name, content) {
		raw, err := json.Marshal(content)
		if err != nil {
			panic(err)
		}
		err = os.WriteFile(dirpath+"/"+name+".json", raw, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
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

func PackageResult(dirpath string, since time.Time, stdout string, stderr string) CalculationResponse {
	response := CalculationResponse{
		Outputs: make(map[string]interface{}),
		Logs:    strings.Split(stdout, "\n"),
		Errors:  strings.Split(stderr, "\n"),
	}
	files := GetChangedFiles(dirpath, since)
	for _, file := range files {
		response.Outputs[filepath.Base(file)] = HandleOutputFile(file)
	}
	return response
}

func HandleOutputFile(file string) interface{} {
	if strings.HasSuffix(file, ".json") {
		data, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		var out interface{}
		err = json.Unmarshal(data, &out)
		return out
	} else {
		return MakeArtefact(file)
	}
}

func GetChangedFiles(dirpath string, since time.Time) []string {
	changed := make([]string, 0)
	files, err := ioutil.ReadDir("/tmp/")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if file.ModTime().After(since) {
			changed = append(changed, file.Name())
		}
	}
	return changed
}

func SendLogs(host string, token string, calculation string, log string) {
	req, err := http.NewRequest("POST", host+"/api/calculations/logs/"+calculation, strings.NewReader(log))
	if err != nil {
		panic(err)
	}
	req.Header.Add("Authorization", "Bearer "+token)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
}

func SendResult(host string, token string, calculation string, response CalculationResponse) {
	data, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", host+"/api/calculations/remote/"+calculation, bytes.NewReader(data))
	if err != nil {
		panic(err)
	}
	req.Header.Add("Authorization", "Bearer "+token)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
}

func MakeArtefact(path string) Artefact {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	contentType := http.DetectContentType(data)
	return Artefact{
		Name:        filepath.Base(path),
		ContentType: contentType,
		URI:         "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(data),
	}
}

func HandleAsArtefact(dirpath string, name string, content interface{}) bool {
	toexpand := content.(map[string]interface{})
	if toexpand["name"] != nil && toexpand["uri"] != nil && toexpand["contentType"] != nil {
		ReadArtefact(dirpath, name, Artefact{
			Name:        toexpand["name"].(string),
			ContentType: toexpand["contentType"].(string),
			URI:         toexpand["uri"].(string),
		})
	}
	return false
}

func ReadArtefact(dirpath string, name string, artefact Artefact) {
	if !strings.HasPrefix(artefact.URI, "data:") {
		panic("Not a data URI")
	}
	b64 := strings.SplitN(artefact.URI, ",", 2)[1]
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		panic(err)
	}
	extension := name[strings.LastIndex(artefact.Name, ".")+1:]
	err = os.WriteFile(dirpath+"/"+name+"."+extension, raw, os.ModePerm)
	if err != nil {
		panic(err)
	}
}
