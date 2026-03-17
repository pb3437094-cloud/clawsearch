import os
from dotenv import load_dotenv
from axiomtradeapi import AxiomTradeClient

load_dotenv()

client = AxiomTradeClient(
    username=os.getenv("EMAIL_ADDRESS"),
    password=os.getenv("AXIOM_PASSWORD")
)

if not client.is_authenticated():
    print("Logging in...")
    client.login()

print("Authenticated as:", client.auth_manager.username)

# Simulate browser connection (important)
client.connect()

print("Connection initialized successfully.")
