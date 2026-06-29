package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

	dynamoClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { //dynamo client, msm jeito do sqs so muda o service
		o.BaseEndpoint = aws.String(endpoint)
	})

	queueURL := getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/barbearia.fifo")
	workerID := getenv("WORKER_ID", "barbeiro-1")
	recepcaoURL := getenv("RECEPCAO_URL", "http://localhost:8080")
	tableName := getenv("DYNAMO_TABLE", "barbearia")

	fmt.Printf("event=worker.started worker_id=%s queue=%s\n", workerID, queueURL)

	for {
		//long polling, segura a conexao ate ter msg ou estourar o timeout, barbeiro dormindo esperando cliente
		sendHeartbeat(recepcaoURL, workerID, "dormindo")

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
			continue //timeout sem msg, volta pro inicio do loop dormir dnv
		}

		msg := output.Messages[0]
		var req AppointmentRequest
		if err := json.Unmarshal([]byte(*msg.Body), &req); err != nil {
			fmt.Println("event=worker.unmarshal_failed error=", err)
			deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
			continue
		}

		//tenta ocupar cadeira no dynamo, se lotou nao deleta msg e ela volta pra fila sozinha pelo visibility timeout
		//poderia encaminharmos ao dlq, tipo clientes que sairam cai na dlq pra validarmos clientes perdidos, e o motivo se der, sla cadeiras cheias
		if !ocuparCadeira(ctx, dynamoClient, tableName) {
			fmt.Printf("event=worker.lotado client=%s\n", req.ClientName)
			time.Sleep(5 * time.Second)
			continue
		}

		sendHeartbeat(recepcaoURL, workerID, "cortando")
		fmt.Printf("event=worker.cutting client=%s haircut=%s\n", req.ClientName, req.Haircut)
		time.Sleep(3 * time.Second) //simula duracao do corte

		//terminou, libera cadeira e deleta msg da fila
		liberarCadeira(ctx, dynamoClient, tableName)
		deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
		sendHeartbeat(recepcaoURL, workerID, "terminou")
		fmt.Printf("event=worker.done client=%s haircut=%s\n", req.ClientName, req.Haircut)
	}
}

// ADD +1 atomico no dynamo, so incrementa se cadeiras_ocupadas < max, se ja lotou retorna ConditionalCheckFailedException
func ocuparCadeira(ctx context.Context, client *dynamodb.Client, table string) bool {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "cadeiras"},
		},
		UpdateExpression: aws.String("ADD cadeiras_ocupadas :inc"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: "1"},
			":max": &types.AttributeValueMemberN{Value: "3"},
		},
		ConditionExpression: aws.String("cadeiras_ocupadas < :max"),
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return false
		}
		fmt.Println("event=dynamo.ocupar_failed error=", err)
		return false
	}
	return true
}

// ADD com valor negativo decrementa, atomico sem lock
func liberarCadeira(ctx context.Context, client *dynamodb.Client, table string) {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "cadeiras"},
		},
		UpdateExpression: aws.String("ADD cadeiras_ocupadas :dec"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":dec": &types.AttributeValueMemberN{Value: "-1"},
		},
	})
	if err != nil {
		fmt.Println("event=dynamo.liberar_failed error=", err)
	}
}

// post pro recepcao informando estado do barbeiro, temq implementar POST /heartbeat e GET /stats la no recepcao pra receber isso
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
