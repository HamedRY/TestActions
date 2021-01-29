from api_access import ApiAccess

services = [
{
    "app": "authentication_service",
    "id": "1",
    "type": "worker",
    "queues": "authentication",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "bot_service",
    "id": "2",
    "type": "worker",
    "queues": "bot",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "customers_service",
    "id": "3",
    "type": "worker",
    "queues": "customers",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "file_service",
    "id": "4",
    "type": "worker",
    "queues": "file",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "identification_service",
    "id": "5",
    "type": "worker",
    "queues": "identification",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "match_service",
    "id": "6",
    "type": "worker",
    "queues": "match",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "odd_service",
    "id": "7",
    "type": "worker",
    "queues": "odd",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "screening_service",
    "id": "8",
    "type": "worker",
    "queues": "screenings",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "strategies_service",
    "id": "9",
    "type": "worker",
    "queues": "strategies",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "verification_service",
    "id": "10",
    "type": "worker",
    "queues": "verification",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "transaction_service",
    "id": "11",
    "type": "worker",
    "queues": "transaction",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "exchange_service",
    "id": "12",
    "type": "worker",
    "queues": "exchange",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "lookup_service",
    "id": "13",
    "type": "beat",
    "queues": "lookup",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "diagnostics_service",
    "id": "14",
    "type": "worker",
    "queues": "diagnostics",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
    "app": "feedback_service",
    "id": "15",
    "type": "worker",
    "queues": "feedbacks",
    "concurrency": "4",
    "backend": "redis://skynet-redis",
    "broker": "redis://skynet-redis",
    "loglevel": "INFO"
},
{
	"app" : "sendgrid_service",
	"id" : "16",
	"type" : "beat",
	"queues" : "sendgrid",
	"concurrency" : "4",
	"backend" : "redis://skynet-redis",
	"broker" : "redis://skynet-redis",
	"loglevel" : "INFO"
},
{
	"app" : "gold_service",
	"id" : "17",
	"type" : "beat",
	"queues" : "gold",
	"concurrency" : "4",
	"backend" : "redis://skynet-redis",
	"broker" : "redis://skynet-redis",
	"loglevel" : "INFO"
},
{
	"app" : "analytics_service",
	"id" : "18",
	"type" : "beat",
	"queues" : "analytics",
	"concurrency" : "4",
	"backend" : "redis://skynet-redis",
	"broker" : "redis://skynet-redis",
	"loglevel" : "INFO"
},
{
	"app" : "payment_service",
	"id" : "19",
	"type" : "beat",
	"queues" : "payment",
	"concurrency" : "4",
	"backend" : "redis://skynet-redis",
	"broker" : "redis://skynet-redis",
	"loglevel" : "INFO"
}
]

if __name__ == "__main__":
    ApiAccess._skynet_db.services.drop()
    ApiAccess._skynet_db.services.insert_many(services)