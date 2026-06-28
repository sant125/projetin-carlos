package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type AppointmentRequest struct {
	ClientName string `json:"client_name"`
	Haircut    string `json:"haircut"`
}

func main() {
	url := flag.String("url", getenvString("PRODUCER_URL", "http://localhost:8080/appointments"), "URL da recepcao")
	total := flag.Int("total", getenvInt("PRODUCER_TOTAL", 10), "quantidade de clientes")
	minDelay := flag.Duration("min-delay", getenvDuration("PRODUCER_MIN_DELAY", 500*time.Millisecond), "menor intervalo de req")
	maxDelay := flag.Duration("max-delay", getenvDuration("PRODUCER_MAX_DELAY", 3*time.Second), "maior intervalo possivel de req")
	validCuts := flag.String("valid-cuts", getenvString("PRODUCER_VALID_CUTS", "moicano,topete,blindadao"), "cortes validos separados por virgula")
	invalidCuts := flag.String("invalid-cuts", getenvString("PRODUCER_INVALID_CUTS", "asa-delta,careca-nevada,mullet"), "cortes invalidos separados por virgula")
	names := flag.String("names", getenvString("PRODUCER_NAMES", "carlos,ana,joao,maria,pedro"), "nomes separados por virgula")
	invalidEvery := flag.Int("invalid-every", getenvInt("PRODUCER_INVALID_EVERY", 4), "a cada N requests envia um corte invalido")
	requestTimeout := flag.Duration("request-timeout", getenvDuration("PRODUCER_REQUEST_TIMEOUT", 5*time.Second), "timeout da chamada HTTP")
	dryRun := flag.Bool("dry-run", getenvBool("PRODUCER_DRY_RUN", false), "oh se eu quisesse")

	flag.Parse()

	validCutList := splitCSV(*validCuts)
	invalidCutList := splitCSV(*invalidCuts)
	nameList := splitCSV(*names)

	if len(validCutList) == 0 {
		fmt.Println("event=producer.error reason=no_valid_cuts")
		os.Exit(1)
	}

	if len(invalidCutList) == 0 {
		fmt.Println("event=producer.error reason=no_invalid_cuts")
		os.Exit(1)
	}

	if len(nameList) == 0 {
		fmt.Println("event=producer.error reason=no_names")
		os.Exit(1)
	}

	if *maxDelay < *minDelay {
		fmt.Println("event=producer.error reason=max_delay_lower_than_min_delay")
		os.Exit(1)
	}

	client := &http.Client{
		Timeout: *requestTimeout,
	}

	fmt.Printf("event=producer.started url=%s total=%d min_delay=%s max_delay=%s dry_run=%t\n", *url, *total, *minDelay, *maxDelay, *dryRun)

	for i := 1; i <= *total; i++ {
		request := AppointmentRequest{
			ClientName: pickRandom(nameList),
			Haircut:    pickHaircut(i, *invalidEvery, validCutList, invalidCutList),
		}

		if *dryRun {
			body, err := json.Marshal(request)
			if err != nil {
				fmt.Printf("event=appointment.marshal_failed request_number=%d error=%q\n", i, err)
			} else {
				fmt.Printf("event=appointment.dry_run request_number=%d body=%s\n", i, body)
			}
		} else {
			sendAppointment(client, *url, i, request)
		}

		if i < *total {
			time.Sleep(randomDelay(*minDelay, *maxDelay))
		}
	}

	fmt.Println("event=producer.finished")
}

func sendAppointment(client *http.Client, url string, requestNumber int, request AppointmentRequest) {
	body, err := json.Marshal(request)
	if err != nil {
		fmt.Printf("event=appointment.marshal_failed request_number=%d error=%q\n", requestNumber, err)
		return
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body)) //ai ele recebe a resp do post no body? tipo body de respota no resp?
	if err != nil {
		fmt.Printf("event=appointment.request_failed request_number=%d client=%s haircut=%s error=%q\n", requestNumber, request.ClientName, request.Haircut, err)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4096)) //limitando o tamangho do body de resposta? 2 body?> um la em cima em resp e oto no respBody
	if err != nil {
		fmt.Printf("event=appointment.response_read_failed request_number=%d status=%d error=%q\n", requestNumber, resp.StatusCode, err)
		return
	}

	fmt.Printf("event=appointment.sent request_number=%d client=%s haircut=%s status=%d response=%s\n", requestNumber, request.ClientName, request.Haircut, resp.StatusCode, string(respBody))
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts)) //pq 0?

	for _, part := range parts { //percorre o array vindo em virgulas, aplicando o trim e appendando no slice de strings
		item := strings.TrimSpace(part) // evita ler "aladia", " santiago" nas flags
		if item != "" {
			items = append(items, item)
		}
	}

	return items //retorna pra body de req no sendAppointmennt
}

func pickRandom(items []string) string {
	return items[rand.IntN(len(items))] //no range de items (nome de clients) smp escolha um aleatorio de 0 a n
}

func pickHaircut(requestNumber, invalidEvery int, validCuts, invalidCuts []string) string {
	if invalidEvery > 0 && requestNumber%invalidEvery == 0 { //so quando for difivisl pelo invalid every, 4 a cada 4 2 a cada 2
		return pickRandom(invalidCuts)
	}

	return pickRandom(validCuts)
}

func randomDelay(minDelay, maxDelay time.Duration) time.Duration {
	if minDelay == maxDelay {
		return minDelay
	}

	return minDelay + time.Duration(rand.Int64N(int64(maxDelay-minDelay))) //pegando int aleatorio entre o min delay e max
}

// helpers usados na inicializacao de cada flag da cli, permitindo usar via env ou flag ja implementado, prioridade da env -> fallback pra flag (defaults)
func getenvString(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	return value
}

func getenvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}

	return parsed
}
