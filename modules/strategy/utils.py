import numpy as np
from typing import Tuple, Dict
from modules.logger import get_logger, setup_global_logging


# 로거 설정
logger = get_logger(__name__)


def retrieve_selected_stocks(pool_result: Tuple, symbol_mapper: Dict) -> Dict:
    if len(pool_result) == 0:
        logger.warning("No result found")
        return {}  # 결과가 없을 경우 빈 딕셔너리 반환

    outcomes = pool_result[0]
    strategy_class_instance = pool_result[1]
    strategy_name = strategy_class_instance.__class__.__name__

    execute_date = outcomes["execute_date"]
    selected_stocks = outcomes["selected_stocks"]
    selected_stock_names = [symbol_mapper[s] for s in selected_stocks]

    stock_ratio = 1 / len(selected_stock_names)
    stocks_ratio = {
        stock_name: f"{stock_ratio:.2%}" for stock_name in selected_stock_names
    }

    performances = outcomes["performance"]

    return {
        "strategy_name": strategy_name,
        "execute_date": execute_date,
        "selected_stocks": selected_stocks,
        "selected_stock_names": selected_stock_names,
        "stocks_ratio": stocks_ratio,
        "annual_return": f"{performances['annual_return']:.2%}",
        "annual_volatility": f"{performances['annual_volatility']:.2%}",
        "sharpe_ratio": np.round(performances['sharpe_ratio'], 5),
        "mdd": f"{performances['mdd']:.2%}",
    }
