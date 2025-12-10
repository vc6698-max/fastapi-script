# fastapi-script

# As API server
uvicorn crawler_api:app --host 0.0.0.0 --port 8000

# Or directly
python crawler_api.py


# Default seeds
curl -X POST http://localhost:8000/start

# Custom config
curl -X POST http://localhost:8000/start \
  -H "Content-Type: application/json" \
  -d '{"max_pages": 50000, "num_workers": 8}'


# Output structure (ready for KVS):
kvs_worker/
├── pt-crawl/
│   ├── __ab/
│   │   └── abc123def456...
│   └── __cd/
│       └── cde789...
└── pt-hosts/
    └── en.wikipedia.org