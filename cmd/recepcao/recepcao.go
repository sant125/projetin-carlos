package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// temq receber a request do producer, ou seja subir o server na porta q pssamos com env, unmarshal na reques criando uma struct q recebe os valores asssaod,s caso esteja errada , retornar 404/malformed json sla, expormos stats pra sabermos como esta as cadeiras/barbeiro, sdk do
func main() {
	//inciializando cortes validos, urlsqs, localstack
	queueURL := getenvString("SQS_QUEUE_URL", "http://localhost:4566/000000000000/barbearia.fifo")
	validCuts := getenvCuts("VALID_CUTS", "moicano, topete, blindadao")
	endpoint := getenvString("AWS_ENDPOINT", "http://localhost:4566")

	//inicializando config pra client for sqs
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx) //pega perm - access key aq ou role
	if err != nil {
		fmt.Println("erro ao carregar as config pra chamar aws api")
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) { //sqs client
		o.BaseEndpoint = aws.String(endpoint)
	})
	dynamoClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	//inciializando srv http , mux é um multiplexer http que faz match em paths e repassa aos handlers(funcoes corretas)
	mux := http.NewServeMux()
	tableName := getenvString("DYNAMO_TABLE", "barbearia") //mesma tabela do worker, single table design — pk diferencia (cadeiras vs worker#id)

	//sincroniza cadeiras com a fila no startup, se a recepcao morreu o counter pode ter ficado sujo
	sincronizarCadeiras(ctx, sqsClient, queueURL, dynamoClient, tableName)

	mux.HandleFunc("POST /appointments", makeAppointmentHandler(validCuts, sqsClient, queueURL, dynamoClient, tableName))
	mux.HandleFunc("POST /heartbeat", makeHeartbeatHandler(dynamoClient, tableName))
	mux.HandleFunc("GET /stats", makeStatsHandler(dynamoClient, tableName))
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

type AppointmentRequest struct {
	ClientName string `json:"client_name"`
	Haircut    string `json:"haircut"`
	CreatedAt  int64  `json:"created_at,omitempty"` //timestamp pra cada msg ser unica na fila fifo (dedup por hash do body)
}

func makeAppointmentHandler(validCuts []string, sqsClient *sqs.Client, queueURL string, dynamoClient *dynamodb.Client, tableName string) http.HandlerFunc {
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
		//tenta ocupar cadeira antes de enfileirar, se lotou nem manda pro sqs
		if !ocuparCadeira(r.Context(), dynamoClient, tableName) {
			http.Error(w, "barbearia lotada cria, volta depois", http.StatusServiceUnavailable)
			return
		}
		req.CreatedAt = time.Now().UnixNano()
		if sendToQueue(r.Context(), sqsClient, queueURL, req) {
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, "agendado cria! corte aceito")
		} else {
			//se falhou ao enfileirar, libera a cadeira q acabou de ocupar
			liberarCadeira(r.Context(), dynamoClient, tableName)
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
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String(string(body)),
		MessageGroupId: aws.String(req.ClientName),
	})
	if err != nil {
		fmt.Println("event=sqs.send_failed error=", err)
		return false
	}
	fmt.Println("event=sqs.sent client=", req.ClientName, "haircut=", req.Haircut)
	return true
}

// sincroniza cadeiras com o numero de msgs na fila, a fila eh a fonte de verdade
func sincronizarCadeiras(ctx context.Context, sqsClient *sqs.Client, queueURL string, dynamoClient *dynamodb.Client, table string) {
	result, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []sqsTypes.QueueAttributeName{
			sqsTypes.QueueAttributeNameApproximateNumberOfMessages,
			sqsTypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		fmt.Println("event=sync_cadeiras.failed error=", err)
		return
	}

	//msgs visiveis (esperando) + nao visiveis (sendo processadas pelo worker)
	visiveis, _ := strconv.Atoi(result.Attributes[string(sqsTypes.QueueAttributeNameApproximateNumberOfMessages)])
	emProcesso, _ := strconv.Atoi(result.Attributes[string(sqsTypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	total := visiveis + emProcesso

	_, err = dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]types.AttributeValue{
			"pk":                &types.AttributeValueMemberS{Value: "cadeiras"},
			"cadeiras_ocupadas": &types.AttributeValueMemberN{Value: strconv.Itoa(total)},
		},
	})
	if err != nil {
		fmt.Println("event=sync_cadeiras.dynamo_failed error=", err)
		return
	}
	fmt.Printf("event=sync_cadeiras ok visiveis=%d em_processo=%d total=%d\n", visiveis, emProcesso, total)
}

type HeartbeatRequest struct {
	WorkerID string `json:"worker_id"`
	Estado   string `json:"estado"`
}

func makeHeartbeatHandler(dynamoClient *dynamodb.Client, tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req HeartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "json invalido badworker", http.StatusBadRequest)
			return
		}

		now := time.Now()
		ttl := now.Add(60 * time.Second)

		_, err := dynamoClient.PutItem(r.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: "worker#" + req.WorkerID},
				"estado":    &types.AttributeValueMemberS{Value: req.Estado},
				"last_seen": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Unix())},
				"ttl":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl.Unix())},
			},
		})
		if err != nil {
			fmt.Println("event=heartbeat.dynamo_failed error=", err)
			http.Error(w, "falha ao gravar heartbeat", http.StatusInternalServerError)
			return
		}

		//worker terminou o corte, libera cadeira
		if req.Estado == "terminou" {
			liberarCadeira(r.Context(), dynamoClient, tableName)
		}

		fmt.Printf("event=heartbeat.received worker_id=%s estado=%s\n", req.WorkerID, req.Estado)
		w.WriteHeader(http.StatusOK)
	}
}

// ADD +1 atomico, so ocupa se tem vaga
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
		ConditionExpression: aws.String("attribute_not_exists(cadeiras_ocupadas) OR cadeiras_ocupadas < :max"),
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

// ADD -1 atomico, so libera se tem cadeira ocupada (nao deixa ir negativo)
func liberarCadeira(ctx context.Context, client *dynamodb.Client, table string) {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "cadeiras"},
		},
		UpdateExpression:    aws.String("ADD cadeiras_ocupadas :dec"),
		ConditionExpression: aws.String("cadeiras_ocupadas > :zero"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":dec":  &types.AttributeValueMemberN{Value: "-1"},
			":zero": &types.AttributeValueMemberN{Value: "0"},
		},
	})
	if err != nil {
		fmt.Println("event=dynamo.liberar_failed error=", err)
	}
}

func makeStatsHandler(dynamoClient *dynamodb.Client, tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		//pega counter de cadeiras
		cadeirasResult, err := dynamoClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "cadeiras"},
			},
		})
		if err != nil {
			fmt.Println("event=stats.cadeiras_failed error=", err)
			http.Error(w, "falha ao buscar cadeiras", http.StatusInternalServerError)
			return
		}

		ocupadas := "0"
		if v, ok := cadeirasResult.Item["cadeiras_ocupadas"]; ok {
			ocupadas = v.(*types.AttributeValueMemberN).Value
		}

		//pega todos os workers
		workersResult, err := dynamoClient.Scan(ctx, &dynamodb.ScanInput{
			TableName:        aws.String(tableName),
			FilterExpression: aws.String("begins_with(pk, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":prefix": &types.AttributeValueMemberS{Value: "worker#"},
			},
		})
		if err != nil {
			fmt.Println("event=stats.workers_failed error=", err)
			http.Error(w, "falha ao buscar workers", http.StatusInternalServerError)
			return
		}

		//converte unix pra horario legivel utc-3
		brZone := time.FixedZone("BRT", -3*60*60)

		var workers []map[string]interface{}
		for _, item := range workersResult.Items {
			ts, _ := strconv.ParseInt(item["last_seen"].(*types.AttributeValueMemberN).Value, 10, 64)
			workers = append(workers, map[string]interface{}{
				"worker_id": item["pk"].(*types.AttributeValueMemberS).Value,
				"estado":    item["estado"].(*types.AttributeValueMemberS).Value,
				"last_seen": time.Unix(ts, 0).In(brZone).Format("02/01/2006 15:04:05"),
			})
		}

		resposta := map[string]interface{}{
			"cadeiras_ocupadas": ocupadas,
			"workers":           workers,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resposta)
	}
}
