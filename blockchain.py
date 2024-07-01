import enum
from typing import Dict, Union


DRPC_API_KEY = '<DRPC_API_KEY>'


class ChainId(enum.Enum):
    OPTIMISM_MAINNET = 10
    POLYGON_ZKEVM_MAINNET = 1101
    ZKSYNC_ERA_MAINNET = 324


class Contract(enum.Enum):
    PANCAKE_SMART_ROUTER = 'pancake_smart_router'


class Token(enum.Enum):
    CAKE = 'cake'
    WETH = 'weth'


class NetworkData:
    def __init__(
        self,
        chain_id: int,
        http_rpc_url: str,
        ws_rpc_url: str,
        addresses: Dict[Union[Contract, Token], str],
    ):
        self.chain_id = chain_id
        self.http_rpc_url = http_rpc_url
        self.ws_rpc_url = ws_rpc_url
        self.addresses = addresses


class BlockchainData:
    NETWORKS = {
        ChainId.OPTIMISM_MAINNET: NetworkData(
            chain_id=ChainId.OPTIMISM_MAINNET.value,
            http_rpc_url=f'https://lb.drpc.org/ogrpc?network=optimism&dkey={DRPC_API_KEY}',
            ws_rpc_url=f'wss://lb.drpc.org/ogws?network=optimism&dkey={DRPC_API_KEY}',
            addresses={
                Contract.PANCAKE_SMART_ROUTER: '0x4A7b5Da61326A6379179b40d00F57E5bbDC962c2',
                Token.CAKE: '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85',
                Token.WETH: '0x4200000000000000000000000000000000000006',
            }
        ),
        ChainId.POLYGON_ZKEVM_MAINNET: NetworkData(
            chain_id=ChainId.POLYGON_ZKEVM_MAINNET.value,
            http_rpc_url=f'https://lb.drpc.org/ogrpc?network=polygon-zkevm&dkey={DRPC_API_KEY}',
            ws_rpc_url=f'wss://lb.drpc.org/ogws?network=polygon-zkevm&dkey={DRPC_API_KEY}',
            addresses={
                Contract.PANCAKE_SMART_ROUTER: '0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86',
                Token.CAKE: '0x0D1E753a25eBda689453309112904807625bEFBe',
                Token.WETH: '0x4F9A0e7FD2Bf6067db6994CF12E4495Df938E6e9'
            },
        ),
        ChainId.ZKSYNC_ERA_MAINNET: NetworkData(
            chain_id=ChainId.ZKSYNC_ERA_MAINNET.value,
            http_rpc_url='https://mainnet.era.zksync.io',
            ws_rpc_url='wss://mainnet.era.zksync.io/ws',
            addresses={
                Contract.PANCAKE_SMART_ROUTER: '0xf8b59f3c3Ab33200ec80a8A58b2aA5F5D2a8944C',
                Token.CAKE: '0x3A287a06c66f9E95a56327185cA2BDF5f031cEcD',
                Token.WETH: '0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91'
            },
        ),
    }

    def __init__(self, chain_id: ChainId):
        if chain_id not in self.NETWORKS:
            raise ValueError(f"Unknown chain: {chain_id}")
        self.data = self.NETWORKS[chain_id]

    def chain_id(self) -> int:
        return self.data.chain_id

    def http_rpc_url(self) -> str:
        return self.data.http_rpc_url

    def ws_rpc_url(self) -> str:
        return self.data.ws_rpc_url

    def get_address(self, entity: Union[Contract, Token]) -> str:
        return self.data.addresses[entity]
