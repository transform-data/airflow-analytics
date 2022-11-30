from transform import MQLClient

class MaterializationRunResult:
    materialization: str
    query_time: int
    result: str

class MaterializationRunner():
    mql = MQLClient()

    def __init__(self, api_key):
        self.mql(api_key)


MaterializationRunner( 'tfdk-McxgUj-HvexFWAAgKtpWqpRTeHYKJRJ')

