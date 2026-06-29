package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type AppointmentRequest struct {
	ClientName string `json:"client_name"`
	Haircut    string `json:"haircut"`
}

func main() {
	ctx := context.Background()               //sabermos a origem do start/pra pararmos toda go-rountine/processos num sigkill
	cfg, err := config.LoadDefaultConfig(ctx) //mesma config do recepcao, pega access key do env/profile
	if err != nil {
		fmt.Println("event=worker.config_failed error=", err)
		os.Exit(1)
	}

	endpoint := getenv("AWS_ENDPOINT", "http://localhost:4566")

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) { //sqs client, msm pattern do recepcao - provavéç recebe essa function com default com options defaults, aqui a gnt personaliza passando localstack
		o.BaseEndpoint = aws.String(endpoint)
	})

	queueURL := getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/barbearia.fifo")
	workerID := getenv("WORKER_ID", "barbeiro-1")
	recepcaoURL := getenv("RECEPCAO_URL", "http://localhost:8080")

	fmt.Printf("event=worker.started worker_id=%s queue=%s\n", workerID, queueURL)

	sendHeartbeat(recepcaoURL, workerID, "dormindo") //avisa q ta dormindo antes de entrar no loop

	for {
		//long polling, segura a conexao ate ter msg ou estourar o timeout
		output, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			fmt.Println("event=worker.receive_failed error=", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(output.Messages) == 0 {
			sendHeartbeat(recepcaoURL, workerID, "dormindo") //so manda dormindo qnd a fila ta vazia de verdade
			continue
		}

		msg := output.Messages[0]
		var req AppointmentRequest
		if err := json.Unmarshal([]byte(*msg.Body), &req); err != nil {
			fmt.Println("event=worker.unmarshal_failed error=", err)
			deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
			continue
		}

		sendHeartbeat(recepcaoURL, workerID, "cortando")
		fmt.Printf("event=worker.cutting client=%s haircut=%s\n", req.ClientName, req.Haircut)
		duracao := 5 + rand.IntN(11) //5 a 15 segundos variando por corte
		time.Sleep(time.Duration(duracao) * time.Second)

		deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
		sendHeartbeat(recepcaoURL, workerID, "terminou")
		fmt.Printf("event=worker.done client=%s haircut=%s\n", req.ClientName, req.Haircut)
		//proximo loop ja faz o ReceiveMessage, se tiver msg vai direto pro corte sem mandar dormindo
	}
}

//post pro recepcao informando estado do barbeiro
func sendHeartbeat(recepcaoURL, workerID, estado string) {
	body := fmt.Sprintf(`{"worker_id":%q,"estado":%q}`, workerID, estado)
	resp, err := http.Post(
		recepcaoURL+"/heartbeat",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		fmt.Printf("event=heartbeat.failed worker_id=%s estado=%s error=%q\n", workerID, estado, err)
		return
	}
	resp.Body.Close()
}

func deleteMessage(ctx context.Context, client *sqs.Client, queueURL string, handle *string) {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: handle,
	})
	if err != nil {
		fmt.Println("event=sqs.delete_failed error=", err)
	}
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
