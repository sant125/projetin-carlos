# tabela unica, pk diferencia tipo de registro (cadeiras, worker#id)
# setei ttl para apagar heartbeats velhos q serão expirados,  
resource "aws_dynamodb_table" "barbearia" {
  name         = "barbearia"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
