curl -X 'POST' \
  'http://localhost:8080/api/v1/variables' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  --user 'admin':'admin' \
  -d '{
  "description": "string",
  "key": "string",
  "value": "string"
}'