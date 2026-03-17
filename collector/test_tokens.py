import os
import json
from dotenv import load_dotenv
from axiomtradeapi import AxiomTradeClient

load_dotenv()

client = AxiomTradeClient(
    username=os.getenv("EMAIL_ADDRESS"),
    password=os.getenv("AXIOM_PASSWORD")
)

print("Logging in...")

if not client.is_authenticated():
    client.login()

# This step is REQUIRED for many endpoints
client.connect()

print("Connected successfully.")

tokens = client.get_trending_tokens()

print("\nTOKEN DATA TYPE:")
print(type(tokens))

print("\nFULL RESPONSE:")
print(json.dumps(tokens, indent=2))
