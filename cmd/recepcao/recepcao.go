package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type AppointmentRequest struct {
	ClientName string `json:"client_name"`
	Haircut    string `json:"haircut"`
}

// temq receber a request do producer, ou seja subir o server na porta q pssamos com env, unmarshal na reques criando uma struct q recebe os valores asssaod,s caso esteja errada , retornar 404/malformed json sla, expormos stats pra sabermos como esta as cadeiras/barbeiro, sdk do
func main() {
	//inicializando config pra client for sqs
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx) //pega perm - access key aq
	if err != nil {
		fmt.Println("erro ao carregar as config pra chamar aws api")
		os.Exit(1)
	}
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) { //sqs client
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})
	//inciializando cortes validos
	queueURL := getenvString("SQS_QUEUE_URL", "http://localhost:4566/000000000000/barbearia")
	validCuts := getenvCuts("VALID_CUTS", "moicano, topete, blindadao")
	mux := http.NewServeMux()
	mux.HandleFunc("POST /appointments", makeAppointmentHandler(validCuts, sqsClient, queueURL)) //preciso passar um http.HandlerFunc pra isso, la embaixo tenhgo acesso ao validCuts
	fmt.Println("subindo o server caralhoou,aceitando post no /appointments")
	log.Fatal(http.ListenAndServe(getenvString("HOST_PORT", ":8080"), mux))
}

func getenvCuts(key, fallback string) []string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return splitCSV(fallback)
	} else {
		return splitCSV(value)
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts)) //slcie vazio com capacidade ate o max de elementos

	for _, part := range parts { //percorre o array vindo em virgulas, aplicando o trim e appendando no slice de strings
		item := strings.TrimSpace(part) // evita ler "aladia", " santiago" nas flags
		if item != "" {
			items = append(items, item)
		}
	}
	return items //retorna cortes validos
}

func getenvString(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	} else {
		return value
	}
}

func makeAppointmentHandler(validCuts []string, sqsClient *sqs.Client, queueURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req AppointmentRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "json invalido, sai fora pnc", http.StatusBadRequest)
			return
		}
		if !slices.Contains(validCuts, req.Haircut) {
			http.Error(w, "corte invalido cria", http.StatusUnprocessableEntity)
			return
		}
		if sendToQueue(r.Context(), sqsClient, queueURL, req) {
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, "agendado cria! corte aceito")
		} else {
			http.Error(w, "falha ao enfileirar", http.StatusInternalServerError)
		}
	}
}

func sendToQueue(ctx context.Context, sqsClient *sqs.Client, queueURL string, req AppointmentRequest) bool {
	body, err := json.Marshal(req)
	if err != nil {
		fmt.Println("event=sqs.marshal_failed error=", err)
		return false
	}
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		fmt.Println("event=sqs.send_failed error=", err)
		return false
	}
	fmt.Println("event=sqs.sent client=", req.ClientName, "haircut=", req.Haircut)
	return true
}
