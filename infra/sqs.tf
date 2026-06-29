# fila fifo, dedup pelo hash do body
resource "aws_sqs_queue" "barbearia" {
  name                        = "barbearia.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 30
}
