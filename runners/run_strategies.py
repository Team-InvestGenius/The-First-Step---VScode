import glob
from datetime import datetime
from modules.utils import read_config
from modules.utils import create_strategy
from modules.strategy.strategy_pool import StrategyPool

if __name__ == "__main__":

    configs = glob.glob("../configs/strategies/kor/*.yaml")
    strategies = [create_strategy(read_config(config)) for config in configs]
    pool = StrategyPool(strategies, "aggressive")  # 위험선호형
    r = pool.execute()
    print(r)

    pool2 = StrategyPool(strategies)  # 위험회피형
    r2 = pool2.execute()
    print(r2)


