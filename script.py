import os
import time
import logging
import requests
from web3 import Web3
from web3.middleware import geth_poa_middleware
from web3.datastructures import AttributeDict
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List

# --- Basic Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger('LumaBridgeOracle')

# --- Constants ---
# In a real-world scenario, this would be a complete ABI.
# For simulation, we only need the event we are listening for.
SOURCE_BRIDGE_ABI = '''
[
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "internalType": "address",
                "name": "sender",
                "type": "address"
            },
            {
                "indexed": true,
                "internalType": "address",
                "name": "recipient",
                "type": "address"
            },
            {
                "indexed": false,
                "internalType": "uint256",
                "name": "amount",
                "type": "uint256"
            },
            {
                "indexed": false,
                "internalType": "uint256",
                "name": "destinationChainId",
                "type": "uint256"
            },
            {
                "indexed": false,
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
            }
        ],
        "name": "TokensLocked",
        "type": "event"
    }
]
'''

DESTINATION_BRIDGE_ABI = '''
[
    {
        "inputs": [
            {
                "internalType": "address",
                "name": "recipient",
                "type": "address"
            },
            {
                "internalType": "uint256",
                "name": "amount",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "sourceNonce",
                "type": "uint256"
            }
        ],
        "name": "mintTokens",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]
'''

class ConfigManager:
    """Manages loading and accessing configuration from environment variables."""
    def __init__(self):
        """Initializes the ConfigManager and loads environment variables from a .env file."""
        load_dotenv()
        self.source_chain_rpc: Optional[str] = os.getenv('SOURCE_CHAIN_RPC')
        self.source_bridge_address: Optional[str] = os.getenv('SOURCE_BRIDGE_ADDRESS')
        self.destination_chain_rpc: Optional[str] = os.getenv('DESTINATION_CHAIN_RPC')
        self.destination_bridge_address: Optional[str] = os.getenv('DESTINATION_BRIDGE_ADDRESS')
        self.oracle_private_key: Optional[str] = os.getenv('ORACLE_PRIVATE_KEY') # For simulation only

        self.validate_config()

    def validate_config(self) -> None:
        """Validates that all necessary configuration variables are present."""
        required_vars = [
            'source_chain_rpc',
            'source_bridge_address',
            'destination_chain_rpc',
            'destination_bridge_address',
            'oracle_private_key'
        ]
        for var in required_vars:
            if not getattr(self, var):
                raise ValueError(f"Missing required environment variable: {var.upper()}")
        logger.info("Configuration loaded and validated successfully.")

class ChainConnector:
    """Handles the connection to a specific blockchain via Web3.py."""
    def __init__(self, rpc_url: str, chain_name: str):
        """
        Initializes a connection to the blockchain.

        Args:
            rpc_url (str): The HTTP RPC endpoint for the blockchain node.
            chain_name (str): A human-readable name for the chain (e.g., 'SourceChain').
        """
        self.chain_name = chain_name
        try:
            self.w3 = Web3(Web3.HTTPProvider(rpc_url))
            # Middleware for PoA chains like Polygon, Goerli, etc.
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            if not self.w3.is_connected():
                raise ConnectionError(f"Failed to connect to {self.chain_name} at {rpc_url}")
            self.chain_id = self.w3.eth.chain_id
            logger.info(f"Successfully connected to {self.chain_name} (Chain ID: {self.chain_id}).")
        except Exception as e:
            logger.error(f"Error connecting to {self.chain_name}: {e}")
            raise

    def get_latest_block_number(self) -> int:
        """Fetches the latest block number from the connected chain."""
        try:
            return self.w3.eth.block_number
        except Exception as e:
            logger.error(f"Failed to get latest block from {self.chain_name}: {e}")
            return 0

class HealthMonitor:
    """Performs periodic health checks on RPC endpoints using the requests library."""
    def __init__(self, rpc_urls: List[str]):
        """
        Args:
            rpc_urls (List[str]): A list of RPC URLs to monitor.
        """
        self.rpc_urls = rpc_urls
        logger.info(f"HealthMonitor initialized for: {', '.join(rpc_urls)}")

    def check_status(self) -> None:
        """Checks the status of each RPC endpoint."""
        for url in self.rpc_urls:
            try:
                # A simple JSON-RPC request to check if the node is responsive
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1
                }
                response = requests.post(url, json=payload, timeout=5)
                response.raise_for_status() # Raise an exception for bad status codes
                if 'result' in response.json():
                    logger.info(f"Health check PASSED for RPC: {url}")
                else:
                    logger.warning(f"Health check FAILED for RPC: {url} - Invalid JSON-RPC response.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Health check FAILED for RPC: {url} - {e}")

class EventListener:
    """Listens for specific events on the source blockchain."""
    def __init__(self, connector: ChainConnector, contract_address: str, contract_abi: str):
        """
        Args:
            connector (ChainConnector): The connector for the source chain.
            contract_address (str): The address of the bridge contract to monitor.
            contract_abi (str): The ABI of the contract, containing the event definition.
        """
        self.connector = connector
        self.contract = self.connector.w3.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=contract_abi
        )
        self.event_filter = self.contract.events.TokensLocked.create_filter(fromBlock='latest')
        logger.info(f"EventListener created for 'TokensLocked' event at {contract_address}")

    def poll_for_events(self) -> List[AttributeDict]:
        """
        Polls the blockchain for new 'TokensLocked' events.

        Returns:
            List[AttributeDict]: A list of new event logs found.
        """
        try:
            new_entries = self.event_filter.get_new_entries()
            if new_entries:
                logger.info(f"Found {len(new_entries)} new 'TokensLocked' event(s).")
            return new_entries
        except Exception as e:
            logger.error(f"Error polling for events on {self.connector.chain_name}: {e}")
            return []

class TransactionProcessor:
    """Processes events from the source chain and simulates minting on the destination chain."""
    def __init__(self, connector: ChainConnector, contract_address: str, contract_abi: str, oracle_pk: str):
        """
        Args:
            connector (ChainConnector): The connector for the destination chain.
            contract_address (str): The address of the destination bridge contract.
            contract_abi (str): The ABI of the destination contract.
            oracle_pk (str): The private key of the oracle account for signing transactions.
        """
        self.connector = connector
        self.contract = self.connector.w3.eth.contract(
            address=Web3.to_checksum_address(contract_address),
            abi=contract_abi
        )
        self.oracle_account = self.connector.w3.eth.account.from_key(oracle_pk)
        logger.info(f"TransactionProcessor initialized for destination contract {contract_address}.")
        logger.info(f"Oracle Address: {self.oracle_account.address}")

    def process_event(self, event: AttributeDict) -> None:
        """
        Constructs and simulates sending a mint transaction based on a source chain event.

        Args:
            event (AttributeDict): The 'TokensLocked' event log.
        """
        try:
            recipient = event['args']['recipient']
            amount = event['args']['amount']
            source_nonce = event['args']['nonce']

            logger.info(f"Processing event with nonce {source_nonce}: Mint {amount} tokens for {recipient}.")

            # 1. Build the transaction
            tx_data = self.contract.functions.mintTokens(
                recipient,
                amount,
                source_nonce
            ).build_transaction({
                'from': self.oracle_account.address,
                'nonce': self.connector.w3.eth.get_transaction_count(self.oracle_account.address),
                'gas': 200000, # Estimated gas limit
                'gasPrice': self.connector.w3.eth.gas_price
            })

            # 2. Sign the transaction
            signed_tx = self.connector.w3.eth.account.sign_transaction(tx_data, self.oracle_account.key)

            # --- SIMULATION --- #
            # In a real system, the next line would send the transaction.
            # tx_hash = self.connector.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            # We will simulate this by logging the details.
            logger.info(f"[SIMULATION] Transaction to mint tokens has been signed.")
            logger.info(f"  - To: {tx_data['to']}")
            logger.info(f"  - From: {tx_data['from']}")
            logger.info(f"  - Nonce: {tx_data['nonce']}")
            logger.info(f"  - Signed TX Hash (simulated): {signed_tx.hash.hex()}")

            # Edge Case: Handle already processed nonces (requires a persistent store in production)
            # For this simulation, we'll just log a check.
            self._check_nonce_processed(source_nonce)

        except Exception as e:
            logger.error(f"Failed to process event and simulate transaction: {e}")

    def _check_nonce_processed(self, nonce: int) -> bool:
        """Simulates checking if a nonce has already been processed."""
        # In a real system, this would query a database (e.g., SELECT 1 FROM processed_nonces WHERE nonce = ?)
        logger.info(f"[DB SIMULATION] Checking if nonce {nonce} has been processed... Assumed not processed.")
        return False

class CrossChainBridgeOracle:
    """The main orchestrator for the cross-chain bridge listening service."""
    def __init__(self):
        """Initializes all components of the oracle."""
        logger.info("Initializing Luma Cross-Chain Bridge Oracle...")
        self.config = ConfigManager()

        # Initialize connectors
        self.source_connector = ChainConnector(self.config.source_chain_rpc, 'SourceChain')
        self.dest_connector = ChainConnector(self.config.destination_chain_rpc, 'DestinationChain')

        # Initialize core components
        self.event_listener = EventListener(
            self.source_connector,
            self.config.source_bridge_address,
            SOURCE_BRIDGE_ABI
        )
        self.tx_processor = TransactionProcessor(
            self.dest_connector,
            self.config.destination_bridge_address,
            DESTINATION_BRIDGE_ABI,
            self.config.oracle_private_key
        )

        # Initialize health monitor
        self.health_monitor = HealthMonitor([
            self.config.source_chain_rpc,
            self.config.destination_chain_rpc
        ])

        self.poll_interval = 15 # seconds
        self.health_check_interval = 60 # seconds

    def run(self) -> None:
        """Starts the main event listening loop of the oracle."""
        logger.info("Oracle is now running. Polling for events...")
        last_health_check = 0

        try:
            while True:
                # Perform periodic health check
                if time.time() - last_health_check > self.health_check_interval:
                    self.health_monitor.check_status()
                    last_health_check = time.time()

                # Poll for events
                events = self.event_listener.poll_for_events()
                if not events:
                    logger.debug(f"No new events found. Sleeping for {self.poll_interval} seconds.")
                else:
                    for event in events:
                        self.tx_processor.process_event(event)

                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("Oracle shutting down gracefully.")
        except Exception as e:
            logger.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)


if __name__ == "__main__":
    # --- EXAMPLE SETUP ---
    # To run this simulation, create a `.env` file in the same directory
    # with the following content (using public testnet details):
    #
    # SOURCE_CHAIN_RPC="https://rpc-mumbai.maticvigil.com/"
    # SOURCE_BRIDGE_ADDRESS="0x2cf831b1A0b52479561571CE3452a202154c8612" # Example address
    # DESTINATION_CHAIN_RPC="https://data-seed-prebsc-1-s1.binance.org:8545/"
    # DESTINATION_BRIDGE_ADDRESS="0x8A594fC2145524458fAc6b1525164Ab725b84803" # Example address
    # ORACLE_PRIVATE_KEY="0x0000000000000000000000000000000000000000000000000000000000000001" # Dummy key for simulation
    #
    try:
        oracle = CrossChainBridgeOracle()
        oracle.run()
    except (ValueError, ConnectionError) as e:
        logger.critical(f"Failed to start the oracle due to a configuration or connection error: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error prevented the oracle from starting: {e}", exc_info=True)
