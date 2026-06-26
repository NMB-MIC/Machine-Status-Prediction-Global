Build and load into kind:
- Put your 4 artifacts under service/models/
- docker build -t alert-eta-service:v1 .
- kind load docker-image alert-eta-service:v1 --name kind

Run locally (optional):
- docker run -p 8080:8080 alert-eta-service:v1
- curl http://localhost:8080/health