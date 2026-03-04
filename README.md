# 💎 Hyperion Mining — Mock MCP Verification Server

A specialized version of the Dataverse MCP Server designed for **verification**. It uses a unique, fictional "Hyperion Mining Operations" dataset with non-internet values to ensure your AI Agent is reading data from the server.

## ✨ Features

- **Unique Dataset** — Hyperion Mining Corp (Sector 7G, Void-Delta, Nebula-X).
- **Verifiable Values** — Specific geological yields and quantum chemist signatures.
- **Zero Configuration** — Works out of the box with `MOCK_MODE` forced to true.
- **English/Turkish Hybrid** — Technical labels are English, while keeping unique identifiers clear.

## 📋 Tables & Data

- **`ms_mining_sites`** — Fictional mining locations (e.g., `Hyperion-Alpha-9`).
- **`ms_staff`** — Hyperion engineers and clearance levels.
- **`ms_extraction_logs`** — Specific mineral yields (expressed in kg with 3 decimal precision).

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/pinarkurtunluoglu/mock-mcp-test
cd mock-mcp-test

# Setup virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Run immediately (No .env required!)
python -m dataverse_mcp
```

## 🧪 Verification Questions to ask your Agent:

1. "List the mining sites from the Hyperion database."
2. "What is the net yield for log `LOG-9912X`?"
3. "Who is the Lead Quantum Geologist at Hyperion?"

---

## 📄 License
MIT
