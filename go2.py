import time  # 用于时间相关操作
import requests  # 用于发送HTTP请求
import os  # 用于执行操作系统相关任务
import sys  # 用于访问与 Python 解释器交互的变量和函数
import numpy as np  # 用于数值计算
import pandas as pd  # 用于数据处理
from binance.um_futures import UMFutures  # 导入币安期货交易库
from binance.error import ClientError  # 导入币安客户端错误处理库
from datetime import datetime, timedelta  # 用于处理日期和时间
from keeplive import keeplive  # 导入保持连接模块
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from ruamel.yaml import YAML
import subprocess  # 用于执行外部命令
import math  # 用于数学计算
import pytz  # 导入 pytz 模块
import aiohttp
import ssl
import asyncio
import copy #深度拷贝
import traceback
import yaml

# 调用保持连接函数
keeplive()


# logging_utils.py
import logging
from logging.handlers import TimedRotatingFileHandler

context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
context.options |= ssl.PROTOCOL_TLS_CLIENT

# 日志过滤器
class IgnoreHttpGetLogsFilter(logging.Filter):
    def filter(self, record):
        http_get_condition = 'GET /logs HTTP/1.1' not in record.getMessage()
        options_request_condition = 'OPTIONS * HTTP/1.1' not in record.getMessage()

      # 返回 True 表示保留日志记录，返回 False 表示过滤掉日志记录
        return http_get_condition and options_request_condition
#return 'GET /logs HTTP/1.1' not in record.getMessage()

def setup_logging():
  # 初始化日志记录器
    logger = logging.getLogger()  
    logger.setLevel(logging.DEBUG)

    log_handler = TimedRotatingFileHandler(os.path.join(BASE_PATH, 'app.log'), when='MIDNIGHT', interval=1, backupCount=10, encoding='utf-8')
    log_handler.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)
  # 添加过滤器到文件处理器
    log_handler.addFilter(IgnoreHttpGetLogsFilter())
  # 控制台日志处理器
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_formatter = logging.Formatter('%(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.addFilter(IgnoreHttpGetLogsFilter()) # 添加过滤器到控制台处理器

    logger.addHandler(log_handler) # 添加文件处理器到日志
    logger.addHandler(stream_handler) # 添加控制台处理器到日志

    return logger

# 基础路径
#BASE_PATH = "C:/Program Files (x86)/Microsoft/langs/"
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# 构建完整文件路径的函数
def build_file_path(file_name):
    return os.path.join(BASE_PATH, file_name)

# 文件名称常量
CONFIG_JSON = "config.json"
CONFIG_A_TXT = "config_a.txt"
CONFIG_B_TXT = "config_b.txt"
ORDER_RECORDS_JSON = "order_records.json"
STATUS_JSON = "status.json"

#通用的数据加载
def load_json_file(file_name, default_value=None):
  file_path = build_file_path(file_name)
  try:
      with open(file_path, 'r', encoding='utf-8') as file:
          if file_path.endswith('.json'):
              return json.load(file)
          elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
              return yaml.safe_load(file)
          else:
              logger.error(f"不支持的文件格式: {file_path}")
  except FileNotFoundError:
      logger.error(f"{file_path} 文件未找到。")
  except (json.JSONDecodeError, yaml.YAMLError) as e:
      logger.error(f"解析 {file_path} 时出现解码错误: {e}")
  except Exception as e:
      logger.error(f"加载 {file_path} 时出现未预期的错误: {e}")
  return default_value if default_value is not None else {}

# 加载配置文件并创建特定交易对的实例
def load_trading_pair(file_name, symbol):
    file_path = build_file_path(file_name)
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    # 检查是否存在该交易对的配置
    if symbol in config:
        return TradingPair(symbol, config[symbol])
    else:
        raise ValueError(f"配置文件中不存在 {symbol} 的配置")

# 更新JSON文件函数
def update_json_file(file_name, data):
    file_path = build_file_path(file_name)
    temp_file = build_file_path(file_name + '.tmp')

    try:
        # 写入临时文件
        with open(temp_file, 'w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

        # 重命名临时文件，覆盖原文件
        os.replace(temp_file, file_path)
        logger.info(f"{file_path} 已成功更新")

    except json.JSONDecodeError as e:
        logger.error(f"JSON 解码错误：{e}")
        # 可以在这里添加额外的恢复或备份逻辑

    except OSError as e:
        logger.error(f"文件操作错误：{e}")
        # 处理文件操作相关的异常

    except Exception as e:
        logger.error(f"更新 {file_path} 时发生未知错误：{e}")
        # 处理其他可能的异常

    finally:
        # 确保在出错时删除临时文件
        if os.path.exists(temp_file):
            os.remove(temp_file)

# 初始化UMFutures客户端
import json

def load_config():
    config = load_json_file('config.json', {})
    if not config or 'binance_api_key' not in config or 'binance_api_secret' not in config:
        logger.error("API key or secret not found in config.")
        sys.exit(1)
    return config



def setup_um_futures_client():
    config = load_config()
    return UMFutures(key=config['binance_api_key'], secret=config['binance_api_secret'])

# 更新status.json
def update_status(key, value):
    status = load_json_file('status.json', {})
    status[key] = value
    try:
      update_json_file('status.json', status)
    except Exception as e:
      logger.error(f"更新status.json时出错: {e}")

# 优化后的status.json读取
import threading

class StatusManager:
    def __init__(self, file_name, trading_pair, save_interval=60, logger=None):
        self.logger = logger
        self.file_path = self.build_file_path(file_name)
        self.trading_pair = trading_pair
        self.save_interval = save_interval
        self.yaml = YAML()
        self.yaml.preserve_quotes = True  # 保留原始引号
        self.yaml.indent(mapping=2, sequence=4, offset=2) 
        self.status = self.load_status()
        self.save_timer = threading.Timer(self.save_interval, self.save_status)
        self.save_timer.start()

    @staticmethod
    def build_file_path(file_name):
        return os.path.join(BASE_PATH, file_name)

    def load_status(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                all_statuses = self.yaml.load(file) or {}
                return all_statuses.get(self.trading_pair, {})
        except Exception as e:
            logger.error(f"读取状态文件时出错: {e}")
            return {}

    def get_status(self, key, default=None):
        return self.status.get(key, default)

    def update_status(self, key, value):
        self.status[key] = value

    def save_status(self):
        temp_file_path = self.file_path + ".tmp"
        try:
            # 读取现有状态
            with open(self.file_path, 'r', encoding='utf-8') as file:
                all_statuses = self.yaml.load(file) or {}
            all_statuses[self.trading_pair] = self.status

            #logger.info(f"正在保存状态: {self.status}")

            # 先写入临时文件
            with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                self.yaml.dump(all_statuses, temp_file)

            # 重命名临时文件为正式文件
            os.replace(temp_file_path, self.file_path)
            if self.logger:
                self.logger.info(f"状态已保存到文件: {self.trading_pair}")


        except Exception as e:
            if self.logger:
                self.logger.error(f"保存状态到文件时出错: {e}")
            # 如果可能，尝试恢复旧文件
            if os.path.exists(temp_file_path):
                os.replace(temp_file_path, self.file_path)
                if self.logger:
                    self.logger.info("已恢复到上一个状态文件版本")

        self.save_timer = threading.Timer(self.save_interval, self.save_status)
        self.save_timer.start()




    def stop_saving(self):
        self.save_timer.cancel()


class TradingPair:
    def __init__(self, name, config):
        self.name = name
        self.config = config
        # 初始化其他需要的属性，例如当前价格、订单状态等

    # 添加处理特定交易对的方法，例如更新价格、执行交易策略等
    def update_price(self):
        # 更新价格的逻辑
        pass

    def execute_strategy(self):
        # 执行交易策略的逻辑
        pass

def load_trading_pairs(file_name):
    config = load_json_file(file_name)
    trading_pairs = {}
    for name, pair_config in config.items():
        trading_pairs[name] = TradingPair(name, pair_config)
    return trading_pairs

#trading_pairs = load_trading_pairs('config.yaml')

def run_powershell_script(file_name):
    # 使用 PowerShell 执行指定脚本
    #script_path = os.path.join(BASE_PATH, "core", file_name)
    script_path = os.path.join(BASE_PATH, file_name)
    if not os.path.isfile(script_path):
      logger.warning(f"PowerShell script '{file_name}' does not exist. Skipping execution.")
      return
    powershell_executable = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    subprocess.run(f"start {powershell_executable} -File {script_path}", check=True, shell=True)
    logger.info(f"Executed PowerShell script '{file_name}'")


# 加载订单记录
def load_order_records():
    global cached_orders
    cached_orders = load_json_file('order_records.json', default_value=[])
    logger.info("订单记录已加载到缓存")

# 更新内存和文件中的订单记录
def update_order_records(order_data):
    global cached_orders
    # 添加新订单数据
    cached_orders.append(order_data)
    # 更新文件
    update_json_file('order_records.json', cached_orders)

# 获取所有活跃订单并更新订单记录
def fetch_and_update_active_orders(symbol):
    try:
        active_orders = client.get_orders(symbol=symbol)
        updated_records = []
        for order in active_orders:
            updated_record = {
                'order_time': order['time'],  # 您可能需要转换时间格式
                'order_id': order['orderId'],
                'type': order['type'],
                'positionSide': order['positionSide'],
                'side': order['side'],
                'Price': order['price'],
                'quantity': order['origQty'],
                'status': order['status']
            }
            updated_records.append(updated_record)
        update_json_file('order_records.json', updated_records)
        logger.info("活跃订单已更新")
        load_order_records()  # 更新内存中的订单记录
    except ClientError as error:
        logger.error(f"获取活跃订单时发生错误: {error}")
        logger.error(f"{error.args[0], error.args[1], error.args[2]}")
    except Exception as e:
        logger.error(f"处理活跃订单时出现未预期的错误: {e}")

# 检查并更新订单的成交状态
def check_and_update_order_status(symbol):
    global cached_orders
    updated = False
    for record in cached_orders:
        if record['status'] in ['NEW', 'PARTIALLY_FILLED']:
            try:
                response = client.query_order(symbol=symbol, orderId=record['order_id'])
                if response:
                    new_status = response['status']
                    if new_status != record['status']:
                        record['status'] = new_status
                        updated = True
                        logger.info(f"订单 {record['order_id']} 状态更新为 {new_status}")
            except ClientError as error:
                logger.error(f"查询订单 {record['order_id']} 状态时出错: {error}")
            except Exception as e:
                logger.error(f"查询订单 {record['order_id']} 状态时出现未预期的错误: {e}")

    if updated:
        update_json_file('order_records.json', cached_orders)

async def visit_url(url):
    # 创建一个不执行证书验证的SSL上下文
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, ssl=ssl_context) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"成功访问 {url}，返回内容：{content}")
                else:
                    logger.error(f"访问 {url} 失败: HTTP状态码 {response.status}")
        except Exception as e:
            logger.error(f"访问 {url} 时发生错误: {e}")

# 使用此函数时请谨慎，并确保您理解相关的安全风险
is_paused = False
# 交易和市场相关参数
symbol = 'ETHUSDT'  #@@ 交易对符号 BTCUSDT ETHUSDT
symbolp = "BTCUSDT_PERP"  # 特定交易产品的标识符
side = 'BUY'  # 交易方向，'BUY' 表示买入操作
order_type = 'LIMIT'  # 订单类型，'LIMIT' 表示限价订单
time_in_force = 'GTC'  # 订单时效，'GTC' 表示订单有效直到取消（Good Till Canceled）
profit1 = 0.01  # 利润参数1，用于计算和设置盈利目标
profit2 = 0.002  # 利润参数2，通常设定为小于 profit1 的值
Trailing_take_profit = 0.005  # 追踪止盈比例，例如 0.005 表示追踪止盈为0.5%
Slippage = 0.02  # 滑点，callback(%)=100slippage,
callback = 100 * Slippage  # 回调比例，例如 100 * Slippage 表示回调为100倍滑点

# 量化交易和策略参数
leverage = 1.01  # 杠杆倍数，用于放大交易头寸
interval = "5m"  # K线周期，例如 '5m' 表示5分钟
interval_ssbb = "3m"  # K线周期，例如 '5m' 表示5分钟
limit = 2880  # 限制获取市场数据时的数据点数量
limit_test = 2880  # 回测获取市场数据时的数据点数量
martingale = 1.01  # 马丁格尔策略倍数，用于在损失后增加投资量
pmom = 66  # 正动量阈值，用于判断市场趋势
nmom = 30  # 负动量阈值，用于判断市场趋势
ssbb_logic_enabled = 1  # ssbb 和 sbsb 逻辑开关
price_change_enabled = 0  # 价格变化逻辑开关
cost_price_logic_enabled = 0  # 成本和价格逻辑开关
technical_indicators_enabled = 1  # 技术指标逻辑开关
macd_signal_enabled = 1  # MACD信号逻辑开关
trading_strategy_enabled = 1 # 追加对冲策略开关

# 订单和仓位管理相关参数
min_quantity = 0.01  #@@ 最小下单量 0.003 0.01
step_size = 0.001  # 订单量调整的步长
max_position_size = 0.1  #@@ 允许的最大持仓量，0.025 0.1
force_reduce = 0  # 是否启用强制减仓，0 表示不启用

# 程序运行和性能参数
max_run_time = 60 * 60 * 24 * 7  # 最大运行时间，以秒为单位
monitoring_interval = 60  # 监测策略条件成立的时间间隔（秒）
order_interval = 60  # 下单操作的时间间隔（秒）
update_interval = 60  # 更新价格的时间间隔（秒）
ltime = 55  # 循环限速时间（秒）
stime = 550  # 交易操作的频率（秒）

# 全局变量和状态维护
current_time = None  # 当前时间，初始设置为 None
rsi = None  # 相对强弱指数（RSI），用于技术分析
mfi = None  # 资金流量指标（MFI），用于分析买卖压力
ema5 = None  # 5周期指数移动平均线（EMA5），用于趋势分析
kline_data_cache = {}  # 存储不同时间间隔K线数据的字典
initial_balance = 0  # 交易账户的初始余额
long_profit, short_profit = 0, 0  # 分别记录多头和空头头寸的利润
cached_orders = [] # 全局变量来存储订单数据
last_known_price = None  # 用于存储最后一次成功获取的价格
last_price_update_time = None  # 用于存储最后一次价格更新的时间
sbsb = 0  # 策略的初始交易开关状态，0 表示关闭

#追加策略
add_position = min_quantity


status_manager = StatusManager('status.yaml', 'ETHUSDT', save_interval=60)
# 从status字典中获取值，并确保它们不是None
starta_direction = status_manager.get_status('starta_direction', 'lb')
add_rate = status_manager.get_status('add_rate', 0.02)  # 追加仓位的跨度
ts_threshold = status_manager.get_status('ts_threshold', 75) #分数阈值
current_config = status_manager.get_status('current_config', 'a')
long_position = status_manager.get_status('long_position', 0)
short_position = status_manager.get_status('short_position', 0)
long_cost = status_manager.get_status('long_cost', 0)
short_cost = status_manager.get_status('short_cost', 0)
floating_margin = status_manager.get_status('floating_margin', 0)
initial_balance = status_manager.get_status('initial_balance', 0)
last_order_price = status_manager.get_status('last_order_price', 0)
last_s_order_price = status_manager.get_status('last_s_order_price', 0)
temp_ssbb = status_manager.get_status('ssbb', 0)
FP = status_manager.get_status('FP', 0.01)
quantity = status_manager.get_status('quantity', min_quantity)
last_order_direction = status_manager.get_status('last_order_direction', 'BUY')

last_price_update_time_str = status_manager.get_status('last_price_update_time', None)
last_config_update_time_str  = status_manager.get_status('last_config_update_time', None)


rsi_trigger_low = status_manager.get_status('rsi_trigger_low', 30)
rsi_trigger_high = status_manager.get_status('rsi_trigger_high', 70)
mfi_trigger_low = status_manager.get_status('mfi_trigger_low', 20)
mfi_trigger_high = status_manager.get_status('mfi_trigger_high', 80)
so_trigger_low = status_manager.get_status('so_trigger_low', 20)
so_trigger_high = status_manager.get_status('so_trigger_high', 80)
fastperiod = status_manager.get_status('fastperiod', 12)
slowperiod = status_manager.get_status('slowperiod', 26)
signalperiod = status_manager.get_status('signalperiod', 9)

rsisold_lower_bound = status_manager.get_status('rsisold_lower_bound', 25)
rsisold_upper_bound = status_manager.get_status('rsisold_upper_bound', 36)
rsibought_lower_bound = status_manager.get_status('rsibought_lower_bound', 67)
rsibought_upper_bound = status_manager.get_status('rsibought_upper_bound', 76)

mfi_low_lower_bound = status_manager.get_status('mfi_low_lower_bound', 15)
mfi_low_upper_bound = status_manager.get_status('mfi_low_upper_bound', 31)
mfi_high_lower_bound = status_manager.get_status('mfi_high_lower_bound', 60)
mfi_high_upper_bound = status_manager.get_status('mfi_high_upper_bound', 81)

fastperiod_lower_bound = status_manager.get_status('fastperiod_lower_bound', 12)
fastperiod_upper_bound = status_manager.get_status('fastperiod_upper_bound', 27)
slowperiod_lower_bound = status_manager.get_status('slowperiod_lower_bound', 26)
slowperiod_upper_bound = status_manager.get_status('slowperiod_upper_bound', 53)
signalperiod_lower_bound = status_manager.get_status('signalperiod_lower_bound', 9)
signalperiod_upper_bound = status_manager.get_status('signalperiod_upper_bound', 18)

average_long_cost = status_manager.get_status('average_long_cost', 0)
average_short_cost = status_manager.get_status('average_short_cost', 0)
average_long_position = status_manager.get_status('average_long_position', 0)
average_short_position = status_manager.get_status('average_short_position', 0)

sosold_lower_bound = status_manager.get_status('sosold_lower_bound', 10)
sosold_upper_bound = status_manager.get_status('sosold_upper_bound', 30)
sobought_lower_bound = status_manager.get_status('sobought_lower_bound',70)
sobought_upper_bound = status_manager.get_status('sobought_upper_bound',90)


starta_price = status_manager.get_status('starta_price', 0)
starta_position = status_manager.get_status('starta_position', add_position)
starta_cost = status_manager.get_status('starta_cost', starta_price)
trade_executed_1 = status_manager.get_status('trade_executed_1', False)
trade_executed_2 = status_manager.get_status('trade_executed_2', False)
trade_executed_3 = status_manager.get_status('trade_executed_3', False)
trade_executed_4 = status_manager.get_status('trade_executed_4', False)
start_price_reached_1 = status_manager.get_status('start_price_reached_1', False)
start_price_reached_2 = status_manager.get_status('start_price_reached_2', False)
start_price_reached_3 = status_manager.get_status('start_price_reached_3', False)
start_price_reached_4 = status_manager.get_status('start_price_reached_4', False)
add_position_1 = status_manager.get_status('add_position_1', 0)
optimal_price_1 = status_manager.get_status('optimal_price_1', 0)
optimal_price_3 = status_manager.get_status('optimal_price_3', 0)  
breakeven_price_2 = status_manager.get_status('breakeven_price_2', 0)
breakeven_price_4 = status_manager.get_status('breakeven_price_4', 0)



def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
  session = session or requests.Session()
  retry = Retry(
      total=retries,
      read=retries,
      connect=retries,
      backoff_factor=backoff_factor,
      status_forcelist=status_forcelist,
  )
  adapter = HTTPAdapter(max_retries=retry)
  session.mount('http://', adapter)
  session.mount('https://', adapter)
  return session

from dateutil.parser import parse
def price_to_datetime(update_time_str):
    try:
        if update_time_str:
            # 使用 dateutil 解析器以处理多种日期时间格式
            update_time = parse(update_time_str)
            logger.info(f"{update_time_str} set to: {update_time}")
        else:
            update_time = None
            logger.info(f"update_time_str: {update_time}")
        return update_time
    except Exception as e:
        logger.error(f"Error parsing {update_time}: {e}")
        update_time = None
        return update_time

ssbb = temp_ssbb 

# 获取公网IP和延迟函数
def get_public_ip_and_latency():
      try:
          response = requests.get('http://ip-api.com/json/', timeout=5)
          data = response.json()
          if data['status'] == 'success':
                public_ip = f"{data.get('query', 'Unknown')}, {data.get('country', 'Unknown')}, {data.get('city', 'Unknown')}, {data.get('isp', 'Unknown')}"

          start_time = time.time()
          requests.get('https://api.binance.com/api/v1/ping', timeout=5)
          latency = round((time.time() - start_time) * 1000,1)
          logger.info(f"Public IP: {public_ip}, Latency: {latency} ms")
          return public_ip, latency
      except requests.RequestException as e:
          logger.error(f"网络请求错误: {e}")
          return 'Unknown', None

# 获取服务器时间函数
def get_binance_server_time():
    try:
        response = requests.get("https://api.binance.com/api/v1/time", timeout=10).json()
        return response.get('serverTime', None)
    except requests.RequestException as e:
        logger.error(f"获取服务器时间时出错: {e}")
        return None

# 获取时间函数
def get_current_time():
    global current_time  # 声明current_time为全局变量
    try:
        current_time = datetime.now()  # 获取当前的datetime对象
        time_str = current_time.strftime("%H:%M:%S")  # 格式化时间字符串
      # 将小时数转换为东八区时间
        current_hour = current_time.hour
        if current_hour < 16:
          current_hour += 0
        else:
          current_hour -= 0
        time_str8 = current_time.strftime(f"{current_hour}:%M:%S")
        logger.info(f"当前时间：{time_str8}")
        return current_time, time_str8  # 返回datetime对象和时间字符串
    except Exception as e:
        print(f"获取时间时出错: {e}")
        return datetime.now(), "Error"  # 发生错误时返回当前时间的datetime对象

# 获取当前价格
def get_current_price(symbol, max_retries=1, now=0):
  global last_known_price, last_price_update_time 
  current_time = datetime.now()

  # 检查是否在更新间隔时间内
  if last_price_update_time is not None and now == 0:
      elapsed_time = (current_time - last_price_update_time).total_seconds()
      if elapsed_time < update_interval:
          logger.info(f"-延用历史价格:{last_known_price}")
          return last_known_price, last_price_update_time

  primary_url = f"https://fapi.binance.com/fapi/v2/ticker/price?symbol={symbol}"
  fallback_url = f"https://api.hbdm.com/linear-swap-ex/market/bbo?contract_code={symbol[:-4]}-{symbol[-4:]}"
  current_url = primary_url

  for attempt in range(max_retries):
      timeout = 10 + 5 * attempt  # 逐步增加超时时间
      try:
        if current_time.minute % 5 in [0, 1]:
          #response = requests.get(current_url, timeout=timeout)
          response = requests_retry_session().get(current_url)
          response.raise_for_status()
          if current_url == primary_url:
            # 如果是主要URL，使用Binance价格
            last_known_price = float(response.json()['price'])
            logger.info(f"get_current_price更新价格{last_known_price}")
            last_price_update_time = current_time
            last_price_update_time_str = last_price_update_time.isoformat()
            status_manager.update_status('last_price_update_time', last_price_update_time_str)
          else:
            # 如果是备用URL，使用Huobi价格
            data = response.json()
            if data['status'] == 'ok' and 'ticks' in data and len(data['ticks']) > 0:
              last_known_price = data['ticks'][0]['ask'][0]
              logger.info(f"get_current_price更新价格{last_known_price}")
              last_price_update_time = current_time
              last_price_update_time_str = last_price_update_time.isoformat()
              status_manager.update_status('last_price_update_time', last_price_update_time_str)

        return last_known_price, last_price_update_time
      except requests.exceptions.HTTPError as e:
          logger.error(f"HTTP请求错误: {e}")
      except requests.exceptions.ConnectionError as e:
          logger.error(f"网络连接错误: {e}")
      except requests.exceptions.Timeout as e:
          logger.error(f"请求超时: {e}")
      except Exception as e:
          logger.error(f"获取当前价格时出错: {e}")

      if current_url == primary_url:
          logger.info(f"切换到备用URL，暂停1,{timeout}s")
          time.sleep(timeout)
          current_url = fallback_url
      else:
          current_url = primary_url
      logger.info(f"暂停2,1s")
      time.sleep(1)

  return last_known_price, last_price_update_time


# 更新持仓成本
def update_position_cost(price, quantity, position_side, operation_type):
  global long_position, short_position, long_cost, short_cost, floating_margin, initial_balance

  price = float(price)
  quantity = float(quantity)

  # 优化：仅当操作为买入/卖出时更新成本
  if position_side == "LONG" and operation_type in ['BUY', 'SELL']:
      if operation_type == 'BUY':
          total_cost = long_cost * long_position + price * quantity
          long_position += quantity
      else:  # 'SELL'
          total_cost = max(0, long_cost * long_position - price * quantity)  # 防止负值
          long_position = max(0, long_position - quantity)  # 防止负值

      long_cost = total_cost / long_position if long_position > 0 else 0

  elif position_side == "SHORT" and operation_type in ['SELL', 'BUY']:
      if operation_type == 'SELL':
          total_cost = short_cost * short_position + price * quantity
          short_position += quantity
      else:  # 'BUY'
          total_cost = max(0, short_cost * short_position - price * quantity)  # 防止负值
          short_position = max(0, short_position - quantity)  # 防止负值

      short_cost = total_cost / short_position if short_position > 0 else 0

  status_manager.update_status('long_position', round(long_position, 2))
  status_manager.update_status('short_position', round(short_position, 2))
  status_manager.update_status('long_cost', round(long_cost, 1))
  status_manager.update_status('short_cost', round(short_cost, 1))

  # 更新浮动利润和余额
  floating_margin = initial_balance + (price - long_cost) * long_position + (short_cost - price) * short_position
  status_manager.update_status('floating_margin', round(floating_margin, 1))

# 获取当前仓位状态
def current_status():
    try:
        # 构建最近订单信息
        last_order_info = []
        logger.info("\n***** 最近交易与仓位 *****")
        if last_order_price is not None:
            last_order_info.append(f"-最近买单: {float(last_order_price):.1f}")
        if last_s_order_price is not None:
            last_order_info.append(f"最近卖单: {float(last_s_order_price):.1f}")
        last_order_info = ", ".join(last_order_info) or "最近订单: None"

        # 构建成本和持仓量信息
        cost_info = f"-多头成本: {float(long_cost):.1f}" if long_cost is not None else "None"
        cost_info += f", 空头成本: {float(short_cost):.1f}" if short_cost is not None else ", None"
        position_info = f"-多头持仓量: {round(long_position, 4)}, 空头持仓量: {round(short_position, 4)}"

        # 最近side和余额
        side_and_balance = f"-最近side: {'l' if last_order_direction == 'BUY' else 's' if last_order_direction == 'SELL' else 'None'}:{average_long_cost if last_order_direction == 'BUY' else average_short_cost:.1f}, 余额: {float(floating_margin):.1f}" if floating_margin is not None else "None"
        logger.info(side_and_balance)
        logger.info(last_order_info)
        logger.info(cost_info)
        logger.info(position_info)

        # 状态信息
#        status_message = "\n".join([
 #           side_and_balance,
  #          last_order_info,
   #         cost_info,
    #        position_info
     #   ])

      #  logger.info(status_message)
    except Exception as e:
        logger.error(f"获取当前状态时出错: {e}")

def calculate_limit_for_interval(interval, days):
    interval_seconds = {
        '1m': 60, '3m': 180, '5m': 300, '15m': 900,
        '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400,
        '6h': 21600, '8h': 28800, '12h': 43200, '1d': 86400,
        '3d': 259200, '1w': 604800
    }

    seconds_per_day = 86400  # 一天有86400秒
    interval_in_seconds = interval_seconds.get(interval)

    if not interval_in_seconds:
        raise ValueError(f"未知的K线间隔: {interval}")

    data_points_per_day = seconds_per_day / interval_in_seconds
    return int(data_points_per_day * days)  # 指定天数的数据点数

def is_force_update_time(update_interval_hours):
    current_hour = datetime.now().hour
    return current_hour % update_interval_hours == 0


from dateutil.parser import isoparse
def calculate_needed_klines(elapsed_time, interval):
  # 将间隔字符串转换为秒
  interval_seconds = {
      '1m': 60, '3m': 180, '5m': 300, '15m': 900,
      '30m': 1800, '1h': 3600, '2h': 7200, '4h': 14400,
      '6h': 21600, '8h': 28800, '12h': 43200, '1d': 86400,
      '3d': 259200, '1w': 604800
  }

  interval_in_seconds = interval_seconds.get(interval)
  if not interval_in_seconds:
      raise ValueError(f"无效的时间间隔: {interval}")

  # 计算所需的新K线数量
  needed_klines = elapsed_time // interval_in_seconds
  if needed_klines > 0:
      needed_klines += 1
  return int(needed_klines)

last_price_update_time_dict = {}  # 存储不同时间间隔K线数据的最后更新时间
# 异步获取K线数据的函数
async def get_kline_data_async(symbol, interval, limit, first_fetch=True):
  global last_known_price, last_price_update_time_dict, kline_data_cache, last_price_update_time
  try:
    current_time = datetime.now()

    last_update_time = last_price_update_time_dict.get(interval)

    if is_force_update_time(6) or first_fetch or (interval not in kline_data_cache):
        new_limit = calculate_limit_for_interval(interval, 1) 

    else:
      if not first_fetch and interval in kline_data_cache:
        if last_update_time:
            elapsed_time = (current_time - last_update_time).total_seconds()
            new_limit = calculate_needed_klines(elapsed_time, interval)
            if new_limit < 1:
                logger.info(f"{interval} K线数据最近已更新，跳过重新获取")
                return kline_data_cache[interval]
        else:
            new_limit = limit
      else:
        new_limit = limit

    async with aiohttp.ClientSession() as session:
        url = f"https://api.binance.com/api/v1/klines?symbol={symbol}&interval={interval}&limit={new_limit}"
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    new_data = await response.json()
                    if new_data:
                        last_known_price = float(new_data[-1][4])
                        logger.info(f"kline更新价格{last_known_price}")
                        current_time = datetime.now()
                        last_price_update_time_dict[interval] = current_time  # 更新特定间隔的时间
                        last_price_update_time = current_time
                        last_price_update_time_string = last_price_update_time.isoformat()
                        status_manager.update_status('last_price_update_time', last_price_update_time_string)

                        if interval in kline_data_cache:
                            existing_data = kline_data_cache[interval]
                            merged_data = merge_kline_data(existing_data, new_data)
                            kline_data_cache[interval] = merged_data
                        else:
                            kline_data_cache[interval] = new_data
                else:
                    logger.error(f"请求失败：{response.status}")
        except Exception as e:
            logger.error(f"获取K线数据时出错: {e}")

    return kline_data_cache[interval]
  except Exception as e:
    logger.error(f"get_kline_data_async出错: {e}")
    return None




# 缓存K线数据的函数
def merge_kline_data(existing_data, new_data):
    if not existing_data:
        return new_data

    # 确定K线数据点的持续时间
    duration = existing_data[0][6] - existing_data[0][0]

    # 创建一个集合来存储已有数据的开始时间，以便快速查找
    existing_start_times = set(data_point[0] for data_point in existing_data)

    # 合并数据，确保持续时间一致并忽略重复的数据点
    for data_point in new_data:
        start_time = data_point[0]
        end_time = data_point[6]

        # 校验持续时间
        if (end_time - start_time) != duration:
            continue  # 如果持续时间不匹配，忽略该数据点

        # 添加新的数据点
        if start_time not in existing_start_times:
            existing_data.append(data_point)

    # 确保合并后的数据仍然是按时间排序的
    merged_data = sorted(existing_data, key=lambda x: x[0])
    return merged_data


async def cache_kline_data(symbol, intervals, limit):
  global kline_data_cache
  for interval in intervals:
      first_fetch = interval not in kline_data_cache
      kline_data_cache[interval] = await get_kline_data_async(symbol, interval, limit, first_fetch)

# 更新K线数据的函数
async def update_kline_data_async(symbol, current_time):
  try:
      update_performed = False
      # Update data every hour at 29 and 59 minutes
      if current_time.minute % 15 == 14:
          await cache_kline_data(symbol, [interval_ssbb], limit)
          logger.info(f"已更新: {interval_ssbb}   {len(kline_data_cache[interval_ssbb])}\n")
          update_performed = True
      # Update data every 5 minutes
      if current_time.minute % 3 == 2:
          await cache_kline_data(symbol, [interval], limit)
          logger.info(f"已更新: {interval}   {len(kline_data_cache[interval])}\n")
          update_performed = True
      if update_performed:
        logger.info(f"待更新数据 \n  {interval_ssbb}: {len(kline_data_cache[interval_ssbb])} \n  {interval}: {len(kline_data_cache[interval])}\n")

  except Exception as e:
      # 在这里捕获可能引发的异常
      logger.error(f"异步更新K线数据时出错: {e}")
#领先指标 (Leading Indicators)：RSI SO CCI Williams%R
 #随机震荡指数stochastic_oscillator 
def c_so(data, k_window=14, d_window=3):
  global kline_data_cache #异步获取K线的数据
  try:
      #data = copy.deepcopy(kline_data_cache[interval_cso])
      if current_price is not None:
        data[-1][4] = current_price
      # 获取最高价、最低价和收盘价
      high_prices = pd.Series([float(entry[2]) for entry in data])
      low_prices = pd.Series([float(entry[3]) for entry in data])
      close_prices = pd.Series([float(entry[4]) for entry in data])

      # 计算%K
      low_min = low_prices.rolling(window=k_window).min()
      high_max = high_prices.rolling(window=k_window).max()
      percent_k = 100 * ((close_prices - low_min) / (high_max - low_min))

      # 计算%D - %K的移动平均
      percent_d = percent_k.rolling(window=d_window).mean()

      return percent_k, percent_d
  except Exception as e:
      logger.error(f"Stochastic Oscillator计算出错: {e}")
      return None, None

async def backtest_so(interval, overbought, oversold, k_window=14, d_window=3, initial_capital=10000):
    global kline_data_cache
    try:
        data = copy.deepcopy(kline_data_cache[interval])
        percent_k, percent_d = c_so(data, k_window, d_window)
        if percent_k is None or percent_d is None:
            return None, None, None

        capital = initial_capital
        shares = 0

        for i in range(len(percent_k)):
            price = float(data[i][4])
            if percent_k[i] > overbought and percent_d[i] > overbought:
                capital += shares * price
                shares = 0
            elif percent_k[i] < oversold and percent_d[i] < oversold:
                shares_to_buy = int(capital / price)
                capital -= shares_to_buy * price
                shares += shares_to_buy
            await asyncio.sleep(0)  # 释放控制权

        final_value = capital + shares * float(data[-1][4])
        total_return = (final_value - initial_capital) / initial_capital
        del data
        return overbought, oversold, total_return
    except Exception as e:
        logger.error(f"Backtest SO出错: {e}")
        return None, None, None

async def sobest():
    global so_trigger_low, so_trigger_high, sosold_lower_bound, sosold_upper_bound, sobought_lower_bound, sobought_upper_bound
    best_return = -np.inf
    oversold_range = range(max(1, sosold_lower_bound), min(100, sosold_upper_bound))
    overbought_range = range(max(1, sobought_lower_bound), min(100, sobought_upper_bound))
    iterations = 0
    max_iterations = 10
    testso = 0
    while iterations < max_iterations:
        for overbought in overbought_range:
            for oversold in oversold_range:
                _, _, total_return = await backtest_so(interval, overbought, oversold)
                if total_return > best_return:
                    best_return = total_return
                    best_overbought = overbought
                    best_oversold = oversold
        testso += 1
        logger.info(f"testso {testso}")
        # 动态调整阈值范围
        overbought_adjusted = False
        oversold_adjusted = False

        if best_overbought in [overbought_range.start, overbought_range.stop - 1]:
            overbought_range = range(max(0, best_overbought - 10), min(100, best_overbought + 10))
            overbought_adjusted = True

        if best_oversold in [oversold_range.start, oversold_range.stop - 1]:
            oversold_range = range(max(0, best_oversold - 10), min(100, best_oversold + 10))
            oversold_adjusted = True

        if not overbought_adjusted and not oversold_adjusted:
            break  # 如果没有调整，则提前结束迭代

        iterations += 1
    so_trigger_low, so_trigger_high = best_oversold, best_overbought
    status_manager.update_status('so_trigger_low', so_trigger_low)
    status_manager.update_status('so_trigger_high', so_trigger_high)
    status_manager.update_status('sosold_lower_bound', sosold_lower_bound)
    status_manager.update_status('sosold_upper_bound', sosold_upper_bound)
    status_manager.update_status('sobought_lower_bound', sobought_lower_bound)
    status_manager.update_status('sobought_upper_bound', sobought_upper_bound)
    logger.info(f"更新后的so触发点：超卖阈值 {best_oversold}, 超买阈值 {best_overbought}")
    return best_overbought, best_oversold, best_return


# RSI计算（领先指标 (Leading Indicators)）
def c_rsi(data, length):
    global kline_data_cache #异步获取K线的数据
    try:
        #data = copy.deepcopy(kline_data_cache[interval])
        if current_price is not None:
          data[-1][4] = current_price

        # 获取历史收盘价
        close_prices = pd.Series([float(entry[4]) for entry in data])

        # 计算RSI

        delta = close_prices.diff()
        gain = (delta.clip(lower=0)).rolling(window=length).mean()
        loss = (-delta.clip(upper=0)).rolling(window=length).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        # 在这里处理可能引发的异常
        logger.error(f"RSI计算出错: {e}")
        return None

# 回测函数，根据RSI超卖阈值进行交易
async def backtest_rsi(interval, oversold_range, overbought_range, length=14, initial_capital=10000):
  #param interval: 时间间隔标识符。
  #param oversold_range: 超卖阈值范围。
  #param overbought_range: 超买阈值范围。
  #param length: RSI计算的长度。
  #param initial_capital: 初始资本。
  #return: 最佳超买、超卖阈值和相应的回报率。
    global kline_data_cache #异步获取K线的数据
    try:
      data = copy.deepcopy(kline_data_cache[interval])
      rsi = c_rsi(data, length)
      if rsi is None:
          return None, None, None

      best_return = -np.inf
      best_oversold_threshold = None
      best_overbought_threshold = None
      for oversold_threshold in oversold_range:
          for overbought_threshold in overbought_range:
              capital = initial_capital
              shares = 0

              for i in range(len(data)):
                  price = float(data[i][4])
                  if rsi[i] < oversold_threshold and capital > 0:
                      shares_to_buy = int(capital / price)
                      capital -= shares_to_buy * price
                      shares += shares_to_buy
                  elif rsi[i] > overbought_threshold and shares > 0:
                      capital += shares * price
                      shares = 0
                  await asyncio.sleep(0)  # 释放控制权

              final_value = capital + shares * float(data[-1][4])
              total_return = (final_value - initial_capital) / initial_capital

              if total_return > best_return:
                  best_return = total_return
                  best_oversold_threshold = oversold_threshold
                  best_overbought_threshold = overbought_threshold
      del data
      return best_oversold_threshold, best_overbought_threshold, best_return
    except Exception as e:
      logger.error(f"RSI计算出错: {e}")
      return None, None, None


async def rsibest():
    global rsi_trigger_low, rsi_trigger_high, rsisold_lower_bound, rsisold_upper_bound, rsibought_lower_bound, rsibought_upper_bound

    try:
      found_optimal = False
      expanded = False  # 标记是否已经扩展过范围
      iterations = 0  # 添加迭代次数计数器
      max_iterations = 10  # 设置最大迭代次数
      testrsi = 0
      while not found_optimal and iterations < max_iterations:
          oversold_range = range(rsisold_lower_bound, rsisold_upper_bound)
          overbought_range = range(rsibought_lower_bound, rsibought_upper_bound)

          best_oversold, best_overbought, best_return = await backtest_rsi(interval, oversold_range, overbought_range)
          logger.info(f"RSI测试范围：超卖 {oversold_range}, 超买 {overbought_range}")
          logger.info(f"最佳超卖阈值: {best_oversold}, 最佳超买阈值: {best_overbought}, 最佳回报率: {best_return:.2%}")
          testrsi += 1
          logger.info(f"testrsi {testrsi}")
          if best_oversold is not None and best_overbought is not None:
              # 检查是否触及测试范围的边界，并相应地调整范围
              expanded = False
              if best_oversold == rsisold_lower_bound or best_oversold == rsisold_upper_bound - 1:
                  rsisold_lower_bound = max(0, best_oversold - 25)
                  rsisold_upper_bound = min(100, best_oversold + 25)
                  expanded = True

              if best_overbought == rsibought_lower_bound or best_overbought == rsibought_upper_bound - 1:
                  rsibought_lower_bound = max(0, best_overbought - 25)
                  rsibought_upper_bound = min(100, best_overbought + 25)
                  expanded = True

              if not expanded:
                  # 找到最优值，更新参数并跳出循环
                  rsi_trigger_low = best_oversold
                  rsi_trigger_high = best_overbought
                  status_manager.update_status('rsi_trigger_low', rsi_trigger_low)
                  status_manager.update_status('rsi_trigger_high', rsi_trigger_high)
                  status_manager.update_status('rsisold_lower_bound', rsisold_lower_bound)
                  status_manager.update_status('rsisold_upper_bound', rsisold_upper_bound)
                  status_manager.update_status('rsibought_lower_bound', rsibought_lower_bound)
                  status_manager.update_status('rsibought_upper_bound', rsibought_upper_bound)
                  logger.info(f"更新后的RSI触发点：超卖阈值 {rsi_trigger_low}, 超买阈值 {rsi_trigger_high}")
                  found_optimal = True
          else:
              logger.info(f"维持原RSI触发点：超卖阈值 {rsi_trigger_low}, 超买阈值 {rsi_trigger_high}")
              break  # 如果没有找到更优的阈值组合，跳出循环

          iterations += 1  # 增加迭代计数

      if iterations >= max_iterations:
          logger.info("达到最大迭代次数，结束搜索, 维持原RSI触发点：超卖阈值 {rsi_trigger_low}, 超买阈值 {rsi_trigger_high}")

          # 可以在这里返回最终确定的最优阈值和相关信息
      return best_oversold, best_overbought, best_return

    except Exception as e:
      logger.error(f"rsibest 函数运行时出现错误: {e}")
      # 可以选择返回默认值或者特定的错误指示
      return rsi_trigger_low, rsi_trigger_high, None

# MFI计算
def c_mfi(data, length):
    global kline_data_cache #异步获取K线的数据
    try:
        #data = copy.deepcopy(kline_data_cache[interval])
        if current_price is not None:
            data[-1][4] = current_price


        typical_price = (pd.Series([float(entry[2]) for entry in data]) +
                         pd.Series([float(entry[3]) for entry in data]) +
                         pd.Series([float(entry[4]) for entry in data])) / 3
        money_flow = typical_price * pd.Series([float(entry[5]) for entry in data])
        positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0)
        negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0)
        mfi = 100 - (100 / (1 + (positive_flow.rolling(length).sum() /
                                 negative_flow.rolling(length).sum())))
        return mfi
    except Exception as e:
        # 在这里处理可能引发的异常
        logger.error(f"MFI计算出错: {e}")
        return None

async def backtest_mfi(interval, mfi_low_range, mfi_high_range, mfi_length=14, initial_capital=10000):
  #param interval: 时间间隔标识符。
  ##param mfi_low_range: MFI超卖阈值范围。
  #param mfi_high_range: MFI超买阈值范围。
  #param mfi_length: MFI计算的长度。
  #param initial_capital: 初始资本。
  #return: 最佳MFI超卖、超买阈值和相应的回报率。
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    mfi = c_mfi(data, mfi_length)
    if mfi is None:
        return None, None, None

    best_return = -np.inf
    best_mfi_low = None
    best_mfi_high = None

    for mfi_low in mfi_low_range:
        for mfi_high in mfi_high_range:
            capital = initial_capital
            shares = 0

            for i in range(len(data)):
                price = float(data[i][4])
                if mfi[i] < mfi_low and capital > 0:
                    shares_to_buy = int(capital / price)
                    capital -= shares_to_buy * price
                    shares += shares_to_buy
                elif mfi[i] > mfi_high and shares > 0:
                    capital += shares * price
                    shares = 0
                await asyncio.sleep(0)  # 释放控制权

            final_value = capital + shares * float(data[-1][4])
            total_return = (final_value - initial_capital) / initial_capital

            if total_return > best_return:
                best_return = total_return
                best_mfi_low = mfi_low
                best_mfi_high = mfi_high
    del data
    return best_mfi_low, best_mfi_high, best_return
  except Exception as e:
    logger.error(f"backtest_mfi出错: {e}")
    return None, None, None


async def mfibest():
    global mfi_trigger_low, mfi_trigger_high, mfi_low_lower_bound, mfi_low_upper_bound, mfi_high_lower_bound, mfi_high_upper_bound
    try:
      initial_mfi_low_lower_bound = mfi_low_lower_bound
      initial_mfi_low_upper_bound = mfi_low_upper_bound
      initial_mfi_high_lower_bound = mfi_high_lower_bound
      initial_mfi_high_upper_bound = mfi_high_upper_bound

      found_optimal = False
      expanded = False  # 标记是否已经扩展过范围
      iterations = 0  # 添加迭代次数计数器
      max_iterations = 10  # 设置最大迭代次数
      testmfi = 0
      while not found_optimal and iterations < max_iterations:
          mfi_low_range = range(mfi_low_lower_bound, mfi_low_upper_bound)
          mfi_high_range = range(mfi_high_lower_bound, mfi_high_upper_bound)

          best_mfi_low, best_mfi_high, best_return = await backtest_mfi(interval, mfi_low_range, mfi_high_range)
          logger.info(f"MFI测试范围：低 {mfi_low_range}, 高 {mfi_high_range}")
          logger.info(f"最佳MFI超卖阈值: {best_mfi_low}, 最佳MFI超买阈值: {best_mfi_high}, 最佳回报率: {best_return:.2%}")
          testmfi += 1
          logger.info(f"testmacd {testmfi}")
          if best_mfi_low is not None and best_mfi_high is not None:
              # 如果已经扩展过范围，并且再次触及边界值，恢复初始范围并停止扩展

              if expanded and (best_mfi_low in [0, 100] or best_mfi_high in [0, 100]):
                  mfi_low_lower_bound = initial_mfi_low_lower_bound
                  mfi_low_upper_bound = initial_mfi_low_upper_bound
                  mfi_high_lower_bound = initial_mfi_high_lower_bound
                  mfi_high_upper_bound = initial_mfi_high_upper_bound
                  found_optimal = True
                  logger.info("触及边界值，恢复初始范围并停止扩展")
              else:
                  # 检查是否触及测试范围的边界，并相应地调整范围
                  expanded = False

                  if best_mfi_low == mfi_low_lower_bound or best_mfi_low == mfi_low_upper_bound - 1:
                      mfi_low_lower_bound = max(0, best_mfi_low - 25)
                      mfi_low_upper_bound = min(100, best_mfi_low + 25)
                      expanded = True
                      logger.info(f"优化超卖阈值 {best_mfi_low}")

                  if best_mfi_high == mfi_high_lower_bound or best_mfi_high == mfi_high_upper_bound - 1:
                      mfi_high_lower_bound = max(0, best_mfi_high - 25)
                      mfi_high_upper_bound = min(100, best_mfi_high + 25)
                      expanded = True
                      logger.info(f"优化超买阈值 {best_mfi_high}")

                  if not expanded:
                      # 更新状态管理器中的状态
                      update_status('mfi_trigger_low', best_mfi_low)
                      update_status('mfi_trigger_high', best_mfi_high)
                      status_manager.update_status('mfi_low_lower_bound', mfi_low_lower_bound)
                      status_manager.update_status('mfi_low_upper_bound', mfi_low_upper_bound)
                      status_manager.update_status('mfi_high_lower_bound', mfi_high_lower_bound)
                      status_manager.update_status('mfi_high_upper_bound', mfi_high_upper_bound)
                      logger.info(f"更新后的MFI触发点：超卖阈值 {best_mfi_low}, 超买阈值 {best_mfi_high}")
                      found_optimal = True
          else:
              logger.info("未找到更优的阈值组合")
              break  # 如果没有找到更优的阈值组合，跳出循环

          iterations += 1  # 增加迭代计数

      if iterations >= max_iterations:
          logger.info("达到最大迭代次数，结束搜索")

          # 可以在这里返回最终确定的最优阈值和相关信息
      return best_mfi_low, best_mfi_high, best_return

    except Exception as e:
      logger.error(f"mfibest出错: {e}")
      return best_mfi_low, best_mfi_high, best_return


# EMA计算
def c_ema(interval, length):
    global kline_data_cache
    try:
        data = kline_data_cache[interval]
        if current_price is not None:
            data[-1][4] = current_price
        close_prices = pd.Series([float(entry[4]) for entry in data])
        return close_prices.ewm(span=length, adjust=False).mean()
    except Exception as e:
        logger.error(f"EMA计算出错: {e}")
        return pd.Series()
# 滞后指标 (Lagging Indicators) MovingAverages MACD BollingerBands 
# macd计算 
def c_macd(interval, fastperiod, slowperiod, signalperiod):
  global kline_data_cache
  try:
      # 首先获取K线数据
      data = kline_data_cache[interval]
      if current_price is not None:
          data[-1][4] = current_price

      # 使用c_ema函数计算EMA
      exp1 = c_ema(interval, fastperiod)
      exp2 = c_ema(interval, slowperiod)
      macd = exp1 - exp2
      signal = macd.ewm(span=signalperiod, adjust=False).mean()

      return macd, signal
  except Exception as e:
      logger.error(f"在c_macd中发生异常：{e}")
      return pd.Series(), pd.Series()


async def backtest_macd(interval, fastperiod_range, slowperiod_range, signalperiod_range, initial_capital=10000):
    best_return = -np.inf
    best_params = None
    data = copy.deepcopy(kline_data_cache[interval])

    for fastperiod in fastperiod_range:
        for slowperiod in slowperiod_range:
            for signalperiod in signalperiod_range:
                macd, signal = c_macd(interval, fastperiod, slowperiod, signalperiod)
                capital = initial_capital
                shares = 0

                for i in range(1, min(len(macd), len(signal))):
                    current_price = float(data[i][4])
                    previous_macd = macd[i - 1]
                    current_macd = macd[i]
                    previous_signal = signal[i - 1]
                    current_signal = signal[i]

                    if current_macd > current_signal and previous_macd <= previous_signal:
                        # 买入逻辑
                        shares_to_buy = int(capital / current_price)
                        capital -= shares_to_buy * current_price
                        shares += shares_to_buy
                    elif current_macd < current_signal and previous_macd >= previous_signal:
                        # 卖出逻辑
                        capital += shares * current_price
                        shares = 0
                    await asyncio.sleep(0)  # 释放控制权

                final_value = capital + shares * float(data[-1][4])
                total_return = (final_value - initial_capital) / initial_capital

                if total_return > best_return:
                    best_return = total_return
                    best_params = (fastperiod, slowperiod, signalperiod)
    if best_params:
        logger.info(f"找到的最佳周期：fastperiod={best_params[0]}, slowperiod={best_params[1]}, signalperiod={best_params[2]}")
        del data
        return best_params, best_return
    else:
        logger.info("未找到有效的MACD参数组合")
        return None, best_return



async def macdbest():
    global fastperiod, slowperiod, signalperiod, fastperiod_lower_bound, fastperiod_upper_bound, slowperiod_lower_bound, slowperiod_upper_bound, signalperiod_lower_bound, signalperiod_upper_bound

    # 初始化最佳参数和回报率
    best_params = (fastperiod, slowperiod, signalperiod)
    best_return = -np.inf

    # 初始化搜索范围
    fastperiod_range = range(fastperiod_lower_bound, fastperiod_upper_bound)
    slowperiod_range = range(slowperiod_lower_bound, slowperiod_upper_bound)
    signalperiod_range = range(signalperiod_lower_bound, signalperiod_upper_bound)
    testmacd = 0
    while True:
        # 执行MACD回测
        current_best_params, current_best_return = await backtest_macd(interval, fastperiod_range, slowperiod_range, signalperiod_range)
        testmacd += 1
        logger.info(f"testmacd{testmacd}")
        # 检查是否找到更好的参数组合
        if current_best_return > best_return:
            best_params = current_best_params
            best_return = current_best_return

            # 更新搜索范围
            if best_params[0] == fastperiod_lower_bound:
              fastperiod_lower_bound = max(1, best_params[0] - 8)
              fastperiod_upper_bound = min(100, best_params[0] + 8)
            elif best_params[0] == fastperiod_upper_bound - 1:
              fastperiod_upper_bound = min(100, best_params[0] + 8)
              fastperiod_lower_bound = max(1, best_params[0] - 8)

            if best_params[1] == slowperiod_lower_bound:
              slowperiod_lower_bound = max(1, best_params[1] - 14)
              slowperiod_upper_bound = min(100, best_params[1] + 14)
            elif best_params[1] == slowperiod_upper_bound - 1:
              slowperiod_upper_bound = min(100, best_params[1] + 14)
              slowperiod_lower_bound = max(1, best_params[1] - 14)

            if best_params[2] == signalperiod_lower_bound:
              signalperiod_lower_bound = max(1, best_params[2] - 5)
              signalperiod_upper_bound = min(100, best_params[2] + 5)
            elif best_params[2] == signalperiod_upper_bound - 1:
              signalperiod_upper_bound = min(100, best_params[2] + 5)
              signalperiod_lower_bound = max(1, best_params[2] - 5)

            fastperiod_range = range(fastperiod_lower_bound, fastperiod_upper_bound)
            slowperiod_range = range(slowperiod_lower_bound, slowperiod_upper_bound)
            signalperiod_range = range(signalperiod_lower_bound, signalperiod_upper_bound)
            logger.info(f"MACD测试范围：快 {fastperiod_range}, 慢 {slowperiod_range}, signal {signalperiod_range}")
            logger.info(f"找到更好的参数组合：{best_params}，回报率：{best_return:.2%}，更新搜索范围。")
        else:
            # 没有找到更好的组合，更新全局参数并退出循环
            fastperiod, slowperiod, signalperiod = best_params
            status_manager.update_status('fastperiod', fastperiod)
            status_manager.update_status('slowperiod', slowperiod)
            status_manager.update_status('signalperiod', signalperiod)
            status_manager.update_status('fastperiod_lower_bound', fastperiod_range.start)
            status_manager.update_status('fastperiod_upper_bound', fastperiod_range.stop - 1)
            status_manager.update_status('slowperiod_lower_bound', slowperiod_range.start)
            status_manager.update_status('slowperiod_upper_bound', slowperiod_range.stop - 1)
            status_manager.update_status('signalperiod_lower_bound', signalperiod_range.start)
            status_manager.update_status('signalperiod_upper_bound', signalperiod_range.stop - 1)
            logger.info(f"未找到更好的MACD参数组合，结束搜索。最终确定的MACD参数：{best_params}，回报率：{best_return:.2%}")
            break

    return best_params

# 支撑线计算
def c_support_resistance(interval):
    global kline_data_cache
    try:
        data = copy.deepcopy(kline_data_cache[interval])
        if current_price is not None:
            data[-1][4] = current_price
        df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)
        pivot_point = (df['high'] + df['low'] + df['close']) / 3
        resistance = (2 * pivot_point) - df['low']
        support = (2 * pivot_point) - df['high']
        return pivot_point.iloc[-1], support.iloc[-1], resistance.iloc[-1]
    except Exception as e:
        logger.error(f"在c_support_resistance中发生异常：{e}")
        return None, None, None

# 计算ATR
def c_atr(interval, period=14):
    global kline_data_cache
    try:
        kline_data = copy.deepcopy(kline_data_cache[interval])
        if current_price is not None:
            kline_data[-1][4] = current_price
        df = pd.DataFrame(kline_data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)
        tr = pd.concat([df['high'] - df['low'], abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr.iloc[-1]
    except Exception as e:
        logger.error(f"在c_atr中发生异常：{e}")
        return None


v_ssbb_count = 0  # 初始化两个计数器
v_sbsb_count = 0
# RMI Trend Sniper函数
def c_ssbb(interval):
    global kline_data_cache, v_ssbb_count, v_sbsb_count, pmom, nmom
    try:
        kline_data = kline_data_cache[interval]
        if current_price is not None:
            kline_data[-1][4] = current_price

        if kline_data:
            rsi = c_rsi(kline_data, 14)
            mfi = c_mfi(kline_data, 14)
            # 设置RSI和MFI的权重
            rsi_weight = 0.4
            mfi_weight = 0.6

            # 计算加权平均值
            rsi_mfi_avg = (rsi * rsi_weight + mfi * mfi_weight) / (rsi_weight + mfi_weight)
            ema5 = c_ema(interval, 5)

            # 初始化一个空的Series来存储v_ssbb和v_sbsb的值
            v_ssbb_series = pd.Series(index=rsi_mfi_avg.index)
            v_sbsb_series = pd.Series(index=rsi_mfi_avg.index)

            for i in range(len(rsi_mfi_avg)):
                if i == 0:
                    v_ssbb_series.iloc[i] = 1  # 初始值设置为1
                elif rsi_mfi_avg.iloc[i] < nmom and ema5.pct_change().iloc[i] < 0:
                    v_ssbb_series.iloc[i] = 0
                    v_sbsb_series.iloc[i] = 1
                    v_ssbb_count += 1
                elif rsi_mfi_avg.iloc[i-1] < pmom and rsi_mfi_avg.iloc[i] > pmom and rsi_mfi_avg.iloc[i] > nmom and ema5.pct_change().iloc[i] > 0:
                    v_ssbb_series.iloc[i] = 1
                    v_sbsb_series.iloc[i] = 1
                    v_ssbb_count += 1
                else:
                    v_ssbb_series.iloc[i] = v_ssbb_series.iloc[i-1]  # 继承前一个值
                    v_sbsb_series.iloc[i] = 0
                    v_sbsb_count += 1

            return v_ssbb_series, v_sbsb_series
        else:
            return pd.Series(), pd.Series()
    except Exception as e:
        logger.error(f"计算v_ssbb时出错: {e}")
        return pd.Series(), pd.Series()

def calculate_score(conditions_enabled):
  global temp_ssbb
  data = kline_data_cache[interval]
  if not data:
      logger.error("无K线数据")
      return temp_ssbb, 0  # 返回当前的 ssbb 和 sbsb=0
  if current_price is not None:
    data[-1][4] = current_price
   # 计算技术指标  
  rsi = c_rsi(data, 14)
  mfi = c_mfi(data, 14)
  ema5 = c_ema(interval, 5)
  macd, signal = c_macd(interval_ssbb, fastperiod, slowperiod, signalperiod)
  percent_k, percent_d = c_so(data, 14, 3)
  v_ssbb, v_sbsb = c_ssbb(interval_ssbb)
  score = 0
  current_order_direction = 'BUY' if v_ssbb.iloc[-1] == 1 else 'SELL'
  conditions = [
      # ssbb 和 sbsb 逻辑
      # 领先指标 RSI MFI Stochastic Oscillator ssbb
      (conditions_enabled['technical_indicators'], rsi.iloc[-1] < rsi_trigger_low, 25, f"RSI {rsi.iloc[-1]:.2f} 低于 {rsi_trigger_low}，加25分"),
      (conditions_enabled['technical_indicators'], rsi.iloc[-1] > rsi_trigger_high, -25, f"RSI {rsi.iloc[-1]:.2f} 超过 {rsi_trigger_high}，减25分"),
      (conditions_enabled['technical_indicators'], mfi.iloc[-1] < mfi_trigger_low, 25, f"MFI {mfi.iloc[-1]:.2f} 低于 {mfi_trigger_low}，加25分"),
      (conditions_enabled['technical_indicators'], mfi.iloc[-1] > mfi_trigger_high, -25, f"MFI {mfi.iloc[-1]:.2f} 超过 {mfi_trigger_high}，减25分"),
      (conditions_enabled['technical_indicators'], percent_k.iloc[-1] > percent_d.iloc[-1] and percent_k.iloc[-1] < so_trigger_high, 25, f"%K {percent_k.iloc[-1]:.2f} 大于%D {percent_d.iloc[-1]:.2f}，且未超买，加25分"),
      (conditions_enabled['technical_indicators'], percent_k.iloc[-1] < percent_d.iloc[-1] and percent_k.iloc[-1] > so_trigger_low, -25, f"%K {percent_k.iloc[-1]:.2f} 小于%D {percent_d.iloc[-1]:.2f}，且未超卖，减25分"),
      (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] == 1 and v_ssbb.iloc[-1] == 1, 50, "ssbb逻辑，加50分"),
      (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] == 1 and v_ssbb.iloc[-1] == 0, -50, "ssbb逻辑，减50分"),
      (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] != 1 and v_ssbb.iloc[-1] == 1, 25, "ssbb逻辑，加25分"),
      (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] != 1 and v_ssbb.iloc[-1] == 0, -25, "ssbb逻辑，减25分"),
      # 价格变化逻辑
      (conditions_enabled['price_change'], last_order_price != 0 and current_price < last_order_price * (1 - 3 * FP), 25, "同向波动超3FP，加25分"),
      (conditions_enabled['price_change'], last_s_order_price != 0 and current_price > last_s_order_price * (1 + 3 * FP), -25, "同向波动超3FP，减25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_order_price != 0 and average_short_cost != 0 and current_price < average_short_cost * (1 - 3 * FP) and current_price < last_order_price * (1 - 3 * FP), 25, "反向波动获利超3FP，加25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_s_order_price != 0 and average_long_cost != 0 and current_price > average_long_cost * (1 + 3 * FP) and current_price > last_s_order_price * (1 + 3 * FP), -25, "反向波动获利超3FP，减25分"),
      # 价格变化逻辑 变化翻2倍
      (conditions_enabled['price_change'], last_order_price != 0 and current_price < last_order_price * (1 - 9 * FP), 25, "同向波动超9FP，加25分"),
      (conditions_enabled['price_change'], last_s_order_price != 0 and current_price > last_s_order_price * (1 + 9 * FP), -25, "同向波动超9FP，减25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_order_price != 0 and average_short_cost != 0 and current_price < average_short_cost * (1 - 9 * FP) and current_price < last_order_price * (1 - 9 * FP), 25, "反向波动获利超9FP，加25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_s_order_price != 0 and average_long_cost != 0 and current_price > average_long_cost * (1 + 9 * FP) and current_price > last_s_order_price * (1 + 9 * FP), -25, "反向波动获利超9FP，减25分"),
      # 价格变化逻辑 变化翻3倍
      (conditions_enabled['price_change'], last_order_price != 0 and current_price < last_order_price * (1 - 21 * FP), 25, "同向波动超21FP，加25分"),
      (conditions_enabled['price_change'], last_s_order_price != 0 and current_price > last_s_order_price * (1 + 21 * FP), -25, "同向波动超21FP，减25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_order_price != 0 and average_short_cost != 0 and current_price < average_short_cost * (1 - 21 * FP) and current_price < last_order_price * (1 - 21 * FP), 25, "反向波动获利超21FP，加25分"),
      (conditions_enabled['price_change'], last_order_direction != current_order_direction and last_s_order_price != 0 and average_long_cost != 0 and current_price > average_long_cost * (1 + 21 * FP) and current_price > last_s_order_price * (1 + 21 * FP), -25, "反向波动获利超21FP，减25分"),
      # 成本和价格逻辑
      (conditions_enabled['cost_price_logic'], long_cost != 0 and current_price > long_cost * (1 + 3 * FP) and current_price > last_s_order_price * (1 + 3 * FP), -25, "获利3FP强制卖出逻辑，减25分"),
      (conditions_enabled['cost_price_logic'], short_cost != 0 and current_price < short_cost * (1 - 3 * FP) and current_price < last_order_price * (1 - 3 * FP), 25, "获利3FP强制买入逻辑，加25分"),


    # 滞后指标 MACD
      (conditions_enabled['macd_signal'], macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] < signal.iloc[-2], 50, f"MACD {macd.iloc[-1]:.2f} 上穿信号线{signal.iloc[-1]:.2f}，加50分"),
      (conditions_enabled['macd_signal'], macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] > signal.iloc[-2], 25, f"MACD {macd.iloc[-1]:.2f} 高于信号线{signal.iloc[-1]:.2f}，加25分"),
      (conditions_enabled['macd_signal'], macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] > signal.iloc[-2], -50, f"MACD {macd.iloc[-1]:.2f} 下穿信号线{signal.iloc[-1]:.2f}，减50分"),
      (conditions_enabled['macd_signal'], macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] < signal.iloc[-2], -25, f"MACD {macd.iloc[-1]:.2f} 低于信号线{signal.iloc[-1]:.2f}，减25分")
  ]
  # 遍历所有条件，并根据启用状态计算得分
  logger.info("\n****** 评分系统 ******")  # 在开始之前添加一个换行符
  for is_enabled, condition, score_value, log_message in conditions:
      if is_enabled and condition:
          score += score_value
          logger.info(f" -{log_message}")
  return score

stop_loss_limit = 0.02  # 停损价格阈值
take_profit_limit = 0.02  # 止盈价格阈值

# 网格系统
def calculate_composite_score(current_price, last_order_price, last_s_order_price, stop_loss_limit, take_profit_limit):
    global temp_ssbb
    data = kline_data_cache[interval]
    if not data:
        logger.error("无K线数据")
        return temp_ssbb, 0  # 返回当前的 ssbb 和 sbsb=0
    if current_price is not None:
      data[-1][4] = current_price
     # 计算技术指标  
    rsi = c_rsi(data, 14)
    mfi = c_mfi(data, 14)
    ema5 = c_ema(interval, 5)
    macd, signal = c_macd(interval_ssbb, fastperiod, slowperiod, signalperiod)
    percent_k, percent_d = c_so(data, 14, 3)
    v_ssbb, v_sbsb = c_ssbb(interval_ssbb)
    ssbb, sbsb = int(v_ssbb.iloc[-1]), int(v_sbsb.iloc[-1])
    logger.info(f"******** 网格系统 ********")

    conditions_enabled = {
      'ssbb_logic': ssbb_logic_enabled,
      'price_change': price_change_enabled,
      'cost_price_logic': cost_price_logic_enabled,
      'technical_indicators': technical_indicators_enabled,
      'macd_signal': macd_signal_enabled
    }
    score = calculate_score(conditions_enabled)


  # 网格思路预筛
    score_threshold = 50 #设置阈值
        # 首先检查分数是否达到阈值
    if abs(score) < score_threshold:
        sbsb = 0
        logger.info(f"\nRSI {rsi.iloc[-1]:.2f} MFI {mfi.iloc[-1]:.2f} MACD {macd.iloc[-1]:.2f} \n%K {percent_k.iloc[-1]:.2f} %D {percent_d.iloc[-1]:.2f}\n总分 {score} 未达到阈值 {score_threshold}，\nsbsb: {sbsb},ssbb: {ssbb}")
        # ssbb, sbsb 不变
    else:
      # 辅助函数，检查价格变化是否显著
      def is_price_change_significant(current_price, ref_price):
        price_change_ratio = abs(current_price - ref_price) / max(current_price, ref_price)
        current_order_direction = 'BUY' if score >= score_threshold else 'SELL'
        significant_change = False
        # 通过一个简化的逻辑表达式来判断价格变化是否显著
        if last_order_direction == current_order_direction:
          if (current_price < ref_price * (1 - FP) and last_order_direction == 'BUY') or \
             (current_price > ref_price * (1 + FP) and last_order_direction == 'SELL'):
              significant_change = True
              logger.info(f"{ref_price}追{current_order_direction}{current_price}")
        else:
          ref_price = ref_price if (average_short_cost == 0 and average_long_cost == 0) else (average_long_cost if average_short_cost == 0 else average_short_cost)

          logger.info(f"价格变化方向不同，更新ref_price为{round(ref_price, 1)}，加多成本{average_long_cost}，加空成本{average_short_cost}")
          if (current_price > ref_price * (1 + FP) and last_order_direction == 'BUY') or \
             (current_price < ref_price * (1 - FP) and last_order_direction == 'SELL'):
              significant_change = True
              logger.info(f"{ref_price}获利{current_order_direction}{current_price}")

        return significant_change, price_change_ratio

      def make_decision(action, significant_change, sbsb_value, ssbb_value):
        if significant_change:
          ssbb, sbsb = ssbb_value, sbsb_value
        else:
          action += "价格变化不足"
          ssbb, sbsb = ssbb_value, 0
        return action, ssbb, sbsb

      sbsb = 0  # 重置 sbsb
      action = "无操作"
      ref_price = last_order_price if last_order_direction == 'BUY' else last_s_order_price
      significant_change, price_change_ratio = is_price_change_significant(current_price, ref_price)

      # Decision-making logic
      if last_order_direction == 'BUY':
          if score >= score_threshold:
              action, ssbb, sbsb = make_decision("买入", significant_change, 1, 1)
          elif score <= -score_threshold:
              action, ssbb, sbsb = make_decision("卖出", significant_change, 1, 0)
      elif last_order_direction == 'SELL':
          if score >= score_threshold:
              action, ssbb, sbsb = make_decision("平空", significant_change, 1, 1)
          elif score <= -score_threshold:
              action, ssbb, sbsb = make_decision("做空", significant_change, 1, 0)
      else:
          logger.info(f"无效的最后订单方向：{last_order_direction}")

      logger.info(f"决策：{action}")
      logger.info(f"分数：{score}，阈值：{score_threshold}，价格变化显著：{significant_change}")
      logger.info(f"价格变化比：{price_change_ratio:.2%}")
      logger.info(f"sbsb: {sbsb},ssbb: {ssbb}")

    # 确定是否更新 ssbb
    if temp_ssbb != ssbb:
        temp_ssbb = ssbb  # 更新临时变量
        status_manager.update_status('ssbb', ssbb)  # 更新文件
        logger.info(f"ssbb updated to {ssbb}")
    return ssbb, sbsb  # 返回 ssbb 和 sbsb 的值

# 计算下一个买单的参数,leverage杠杆倍数暂时没有用
def calculate_next_order_parameters(price, leverage):
    global last_order_direction
    try:
        next_price = round(price * (1 - Slippage if ssbb == 1 else 1 + Slippage), 1)
        reference_price = last_order_price if last_order_direction == 'BUY' else last_s_order_price

        if reference_price and reference_price != 0 and next_price != reference_price:
          grid_ratio = max(current_price, reference_price) / min(current_price, reference_price)
          grid_count = int(math.log(grid_ratio) / math.log(1 + FP))
        elif reference_price == 0:
          grid_count = 1
          logger.info(f"首次{last_order_direction}下单: {grid_count}\n")
        else:
          grid_count = 0 #差异小于阈值FP
          logger.info(f"差异小于阈值FP: {grid_count}\n")
        if grid_count > 6:
          logger.info(f"grid:{grid_count}/{grid_ratio}/{1 + FP}")
          grid_count = 6
        # 根据网格数量调整下单量
        origQty = adjust_quantity(quantity * grid_count)

        return next_price, origQty
    except Exception as e:
        logger.error(f"执行calculate_next_order_parameters出错: {e}")
        return None, None  # 确保在出错的情况下返回两个值


def adjust_quantity(ad_quantity):
 #调整下单量以满足最小值和步进要求。:param quantity: 原始下单量"""

    # 确保下单量至少为最小值
    if  ad_quantity < min_quantity:
        ad_quantity = 0
        logger.error(f"quantity小于最小下单量: {ad_quantity}")
    else:
        # 调整为符合步进大小的最接近值
        ad_quantity = math.ceil(ad_quantity / step_size) * step_size

    return ad_quantity


def update_order_status(response, position):
      global stime, ltime, long_position, short_position, last_order_price, last_s_order_price, FP, quantity, last_order_direction, average_long_cost, average_short_cost, average_long_position, average_short_position
      if 'price' in response and 'origQty' in response and 'type' in response and 'side' in response:
        update_price = float(response['price'] if response['type'] != 'TRAILING_STOP_MARKET' else response['activatePrice'])
        quantity = float(response['origQty'])
        logger.info(f"#######################\n=======================\n#######################\n=======================\ncpo成功{response['side']}：{update_price}")
        update_position_cost(update_price, quantity, response['positionSide'], response['side'])
        # 优化：使用激活价格(activatePrice)作为平仓订单的价格
        # 检查交易方向并更新平均成本和持仓量
        if response['side'] == 'BUY':
            if last_order_direction == 'SELL':
                average_short_cost, average_short_position = 0, 0
            else:
                logger.info(f"类型 - update_price: {type(update_price)}, quantity: {type(quantity)}, average_long_cost: {type(average_long_cost)}, average_long_position: {type(average_long_position)}")
                logger.info(f"值 - update_price: {update_price}, quantity: {quantity}, average_long_cost: {average_long_cost}, average_long_position: {average_long_position}")

                average_long_cost, average_long_position = (round((average_long_cost * average_long_position + update_price * quantity) / (average_long_position + quantity), 1),(average_long_position + quantity)) if average_long_cost != 0 else (update_price, quantity)
        else:
            if last_order_direction == 'BUY':
                average_long_cost, average_long_position = 0, 0
            else:
                average_short_cost, average_short_position = (round((average_short_cost * average_short_position + update_price * quantity) / (average_short_position + quantity), 1),(average_short_position + quantity)) if average_short_cost != 0 else (update_price, quantity)
        status_manager.update_status('average_long_cost', average_long_cost)
        status_manager.update_status('average_short_cost', average_short_cost)
        status_manager.update_status('average_long_position', average_long_position)
        status_manager.update_status('average_short_position', average_short_position)
        logger.info(f"update_price to last_order{update_price}")
        if position in ('lb', 'sb'):
          last_order_price = update_price
        elif position in ('ls', 'ss'):
          last_s_order_price = update_price
        status_manager.update_status('last_order_price', last_order_price)
        status_manager.update_status('last_s_order_price', last_s_order_price)
        quantity = float(quantity - min_quantity) * float(martingale) + min_quantity
        status_manager.update_status('quantity', quantity)
        FP = float(FP) * float(leverage)
        status_manager.update_status('FP', FP)
        last_order_direction = response['side']
        status_manager.update_status('last_order_direction', last_order_direction)
        time_str = current_time.strftime("%H:%M:%S")  # 格式化时间字符串
        logger.info(
            "\n" + "=" * 50 +
            f"\n== 订单时间: {time_str} " + " " * (30 - len(time_str)) + "==\n" +
            f"== 当前价格: {current_price:.2f} " + " " * (37 - len(str(current_price))) + "==\n" +
            f"== 数量: {response['origQty']} | 方向: {position} | 价格: {update_price:.2f} " + " " * (10 - len(str(update_price)) - len(str(response['origQty'])) - len(position)) + "==\n" +
            f"== 订单ID: {response['orderId']} " + " " * (40 - len(str(response['orderId']))) + "==\n" +
            "=" * 50 + "\n"
        )

        # 新增订单记录逻辑
        order_record = {
            'order_time': time_str,
            'order_id': response['orderId'],
            'type': response['type'],
            'positionSide': response['positionSide'],
            'side': response['side'],
            'Price': response['price'],
            'quantity': response['origQty'],
            'status': response['status']  # 初始状态设置为未成交
        }
        update_order_records(order_record)
        logger.info(f"暂停3,{order_interval}")
        time.sleep(order_interval )  # 等待12秒

# 下单交易
def place_limit_order(symbol, position, price, quantitya, callback):

    global stime, ltime, long_position, short_position, last_order_price, last_s_order_price, client, logger, FP, quantity, last_order_direction
    if is_paused:
        logger.info("脚本暂停执行")
        return
    origQty = quantitya
    price = round(price * (1 - Slippage if position == 'lb' else 1 + Slippage), 1)
    if origQty <= 0:
        logger.error("下单量必须大于0")
        return NONE

    # 根据持仓量调整下单逻辑
    def determine_order_side():
      # 定义位置状态的常量
      LONG_BUY = 'lb'
      SHORT_SELL = 'ss'
      NONE = 'none'

      if position not in [LONG_BUY, SHORT_SELL]:
          logging.error(f"无效的订单意图：{position}")
          return NONE

      if force_reduce or position == LONG_BUY and long_position > max_position_size or \
         position == SHORT_SELL and short_position > max_position_size:
          return ('BUY', 'SHORT') if position == LONG_BUY else ('SELL', 'LONG')
      return ('BUY', 'LONG') if position == LONG_BUY else ('SELL', 'SHORT')

    order_side, position_side = determine_order_side()

    def create_trailing_stop_order_params(order_side, position_side, price, callback, origQty):
      return {
          'symbol': symbol,
          'side': order_side,
          'positionSide': position_side,
          'type': "TRAILING_STOP_MARKET",
          'activationPrice': round(price * (1 - 0.02 * callback) if order_side == 'BUY' else price * (1 + 0.02 * callback), 1),
          'callbackRate': callback,
          'quantity': math.floor(origQty * 1000) / 1000,
          'timeInForce': time_in_force,
      }

    def create_standard_order_params(order_side, position_side, price, origQty):
      return {
          'symbol': symbol,
          'side': order_side,
          'positionSide': position_side,
          'type': order_type,
          'timeInForce': time_in_force,
          'quantity': math.floor(origQty * 1000) / 1000,
          'price': round(price, 1),
          'priceProtect': False,
          'closePosition': False
      }

    def create_take_profit_order_params(order_side, position_side, price, origQty):
      return {
          'symbol': symbol,
          'side': order_side,
          'positionSide': position_side,
          'type': "TAKE_PROFIT", # STOP, TAKE_PROFIT
          'stopPrice': round(price * (1 - 0.005 * callback) if order_side == 'BUY' else price * (1 + 0.005 * callback), 1),
          'price': round(price * (1 - 0.02 * callback) if order_side == 'BUY' else price * (1 + 0.02 * callback), 1),
          'quantity': math.floor(origQty * 1000) / 1000,
          'timeInForce': time_in_force,
      }

    if order_side == 'none':
        logger.error("order_side为none，停止下单")
        return
    if (order_side == 'BUY' and position_side == 'SHORT') or (order_side == 'SELL' and position_side == 'LONG'):
      order_params = create_trailing_stop_order_params(order_side, position_side, price, callback, origQty)
    else:
      order_params = create_standard_order_params(order_side, position_side, price, origQty)

    try:
        current_time, time_str = get_current_time()  # 获取时间和时间字符串
        current_price, last_price_update_time = get_current_price(symbol, now=1)
        get_binance_server_time()
        response = client.new_order(**order_params)
        return response
    except ClientError as error:
        logger.info(f"w1: {error.args[1]}")
        if error.args[1] == -4045:
            # 当达到最大止损订单限制时，转为市价建仓方式下单
            order_params = create_standard_order_params(order_side, position_side, price, origQty)
            try:
                update_order_status(order_params, position)
                logger.info(f"止损单限制，转为市价建仓方式下单")
                return order_params
            except Exception as e:
                logger.error(f"市价建仓下单时发生错误: {e}")
                logger.error(f"堆栈跟踪: {traceback.format_exc()}")
                time.sleep(monitoring_interval)  # 发生错误时等待一分钟
        else:
            logger.error(f"下单时出错。状态: {error.args[0]}, 错误码: {error.args[1]}, 错误消息: {error.args[2]}, 最新价格：{current_price}, 下单参数: {order_params}")
            logger.error(f"API错误: {error}")
        logger.info(f"暂停4,12s")
        time.sleep(12)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.info(f"暂停5,12s")
        time.sleep(12)

# 查询订单
def query_order(order_id):
    try:
        response = client.query_order(symbol=symbol, orderId=order_id, recvWindow=5000)
        logger.info(response)
    except ClientError as error:
        logger.info(f"w2: {error}")
        logger.error(f"查询订单时出错。状态: {error.status_code}, 错误码: {error.error_code}, 错误消息: {error.error_message}")
    except Exception as e:
        logger.error(f"查询订单时发生未预期的错误: {e}")


def handle_client_error(error, current_price, order_params):
    error_messages = {
        -1021: "时间同步问题",
        400: "请求过多或订单超时",
        -1003: "请求过多或订单超时"
    }
    logger.info(f"w3: {error}")
    logger.error(f"下单时出错。状态: {error.status_code}, 错误码: {error.error_code}, 错误消息: {error.error_message}, 最新价格：{current_price}, 下单参数: {order_params}")

    error_message = error_messages.get(error.error_code, "未知错误")
    logger.error(f"发生错误: {error_message}")

    if error.error_code in [-1021, 400, -1003]:
        logger.info(f"暂停6,{ltime}")
        time.sleep(ltime)
    else:
        logger.info(f"暂停7,{12}")
        time.sleep(12)
# 动态追踪trade_direction=lb买入
def dynamic_tracking(symbol, trade_direction, callback_rate, optimal_price, trade_quantity, start_order_price=None, start_price_reached=False, trade_executed = False):
    global current_price, add_position_1
    try:
        logger.info(f"****** 动态追{trade_direction} ******")
        logger.info(f"触发{start_order_price}:{start_price_reached}成交{trade_executed}回调率{callback_rate}最佳{optimal_price}量{trade_quantity}")
        # 参数校验
        if trade_direction not in ['lb', 'ss']:
          raise ValueError(f"交易方向{trade_direction}必须是 'lb' 或 'ss'")
        if not all(isinstance(x, (int, float)) and x > 0 for x in [callback_rate, optimal_price, trade_quantity]):
          raise ValueError(f"回调率{callback_rate}、最佳价格{optimal_price}和交易数量{trade_quantity}必须是正数")
        if start_order_price is not None and not isinstance(start_order_price, (int, float)):
          raise ValueError(f"启动价格{start_order_price}必须是数字")
        if trade_executed == True:
          raise ValueError("交易已执行，请勿重复执行")
      # 获取当前市场价格
        current_price, last_price_update_time = get_current_price(symbol)

        # 检查当前价格是否达到或超过启动价格
        if start_order_price == None and start_price_reached == False:
          start_order_price = round(optimal_price * (1 - add_rate if trade_direction == 'lb' else 1 + add_rate), 1)
          logger.info(f"启动价为None并未触发则用最佳价格的{add_rate}：{start_order_price}")

        if (trade_direction == 'lb' and start_order_price != None and current_price <= start_order_price) or \
      (trade_direction == 'ss' and start_order_price != None and current_price >= start_order_price):
          start_price_reached = True
          logger.info(f"触发启动价格：{start_order_price}")
        if start_price_reached:
          # 计算价格变化
          price_change = abs(current_price - optimal_price) / optimal_price

          # 检查是否达到回调率阈值
          if price_change >= callback_rate:
              if trade_direction == 'lb' and current_price > optimal_price:
                  # 价格下跌到达回调率，执行买入操作
                  if place_limit_order(symbol, trade_direction, current_price, trade_quantity, callback):
                    trade_executed = True
                    start_price_reached = False
                    optimal_price = 0
                    add_position_1 = 0
              elif trade_direction == 'ss' and current_price < optimal_price:
                  # 价格上涨到达回调率，执行卖出操作
                  if place_limit_order(symbol, trade_direction, current_price, trade_quantity, callback):
                    trade_executed = True
                    start_price_reached = False
                    optimal_price = 0
                    add_position_1 = 0
              else:
                  logger.error("无效的交易方向")
          status_manager.update_status('add_position_1', add_position_1)
          # 如果当前价格更有利，则更新 optimal_price
          if (trade_direction == 'lb' and current_price < optimal_price) or \
             (trade_direction == 'ss' and current_price > optimal_price):
              optimal_price = current_price
              logger.info(f"更新最佳价格：{optimal_price}")
        return start_price_reached, trade_executed, optimal_price
    except ValueError as e:
        logger.error(f"参数非法: {e}")
        return start_price_reached, trade_executed, optimal_price
    except Exception as e:
        logger.error(f"执行动态追踪时发生错误: {e}")
        return False, True, optimal_price



# 保本止盈trade_direction=lb买入
def breakeven_stop_profit(symbol, trade_direction, breakeven_price, trade_quantity, start_order_price=None, start_price_reached=False, trade_executed = False):
  global current_price
  try:
    logger.info(f"执行保本止盈{trade_direction}")
    logger.info(f"触发{start_price_reached}:{start_order_price}成交{trade_executed}保本{breakeven_price}量{trade_quantity}")
    # 参数校验
    if trade_direction not in ['lb', 'ss']:
        raise ValueError(f"交易方向{trade_direction}必须是 'lb' 或 'ss'")
    if breakeven_price <= 0 or trade_quantity <= 0:
      raise ValueError(f"保本价格{breakeven_price}和交易数量{trade_quantity}必须是大于0的正数")

    if start_order_price is not None and start_order_price <= 0:
      raise ValueError(f"启动价格{start_order_price}必须是大于0的正数")

    if trade_quantity < min_quantity:
      raise ValueError(f"交易数量{trade_quantity}必须大于等于最小下单量 (min_quantity)")

    #logger.info(f"类型 - trade_quantity: {type(trade_quantity)}, 值 - trade_quantity: {trade_quantity}")
    decimal_places = 3
    trade_quantity_float = float(trade_quantity)
    step_size_float = float(step_size)
    adjusted_quantity = round(trade_quantity_float / step_size_float, decimal_places)
    if not adjusted_quantity.is_integer():
      raise ValueError(f"交易数量{trade_quantity}必须是最小调整步长{step_size}的整数倍")

    if trade_executed == True:
      raise ValueError("交易已执行，请勿重复执行")
    # 获取当前市场价格
    current_price, last_price_update_time = get_current_price(symbol)

    # 检查当前价格是否达到或超过启动价格
    if start_order_price is not None:
      if not isinstance(start_order_price, (int, float)):
          raise ValueError("启动价格必须是数字")
      start_price_reached = (trade_direction == 'lb' and current_price <= start_order_price) or \
                            (trade_direction == 'ss' and current_price >= start_order_price)
    else:
      start_price_reached = True
    if start_price_reached:
      logger.info(f"触发启动价格：{start_order_price}")
      if (trade_direction == 'lb' and current_price >= breakeven_price) or \
       (trade_direction == 'ss' and current_price <= breakeven_price):
        # 执行止盈操作
        logger.info(f"达到保本价，执行{trade_direction}操作. 价格: {current_price}")
        if place_limit_order(symbol, trade_direction, current_price, trade_quantity, callback):
          trade_executed = True
          start_price_reached = False
      else:
        logger.info("当前价格未达到保本止盈点")
    return start_price_reached, trade_executed
  except ValueError as e:
    logger.error(f"参数错误: {e}")
    return start_price_reached, trade_executed
  except Exception as e:
    logger.error(f"执行保本止盈时发生错误: {e}")
    return False, True

def trading_strategy():
    global starta_price, starta_position, starta_cost, trade_executed_1, start_price_reached_1, optimal_price_1, trade_executed_2, start_price_reached_2, breakeven_price_2, trade_executed_3, start_price_reached_3, optimal_price_3, trade_executed_4, start_price_reached_4, breakeven_price_4, add_position_1
    logger.info("******** 对冲系统 ********")
    try:
      if trading_strategy_enabled == 0:
        logger.info("交易策略未启用")
        return  
      trigger_price = round((min(starta_price if starta_cost == 0 or starta_cost is None else starta_cost, starta_price) if starta_direction == 'lb' else max(starta_cost, starta_price)) * (1 - add_rate if starta_direction == 'lb' else 1 + add_rate), 1)
      profit_price = round(starta_cost * (1 - add_rate if starta_direction == 'ss' else 1 + add_rate), 1)
      logger.info(f"-开始对冲策略，当前价格：{current_price}, 启动价格：{starta_price}")
      logger.info(f"-启动仓位：{starta_position}, 启动成本：{starta_cost}")
      logger.info(f"-追加幅度：{add_rate}, 单位追加量：{add_position}")
      logger.info(f"-追加价格：{trigger_price}, 止盈价格：{profit_price}")
      if starta_price == 0:
        starta_price, last_price_update_time = get_current_price(symbol)
        logger.info(f"启动价格调整为当前价格：{starta_price}")
      if starta_cost == 0:
        starta_cost = starta_price
        logger.info(f"启动成本调整为启动价格：{starta_cost}")
      # 执行交易策略的核心逻辑
      conditions_enabled = {
        'ssbb_logic': ssbb_logic_enabled,
        'price_change': price_change_enabled,
        'cost_price_logic': cost_price_logic_enabled,
        'technical_indicators': technical_indicators_enabled,
        'macd_signal': macd_signal_enabled
      }
      score = calculate_score(conditions_enabled)
      add_direction = 'lb'if starta_direction == 'ss' else 'ss'
      if not trade_executed_1 and add_position_1 != 0:
        logger.info(f"动态追踪1{trade_executed_1}，触发状态{start_price_reached_1}：最佳价格{optimal_price_1}")
        start_price_reached_1, trade_executed_1, optimal_price_1 = dynamic_tracking(symbol, starta_direction, add_rate, optimal_price_1, add_position_1, start_price_reached=start_price_reached_1, trade_executed = trade_executed_1)
        logger.info(f"动态追踪1{trade_executed_1}，触发状态{start_price_reached_1}：最佳价格{optimal_price_1}")
      if trade_executed_1 and add_position_1 != 0:
        starta_position += add_position_1  # 更新持仓量
        add_position_1 = 0
        trade_executed_2 = True
        logger.info(f"动态追踪1{trade_executed_1}，重置交易量,保本止盈2改{trade_executed_2}")
      if not trade_executed_2 and breakeven_price_2 != 0:
        logger.info(f"保本止盈2{trade_executed_2}，触发状态{start_price_reached_2}：保本价格{breakeven_price_2}")
        start_price_reached_2, trade_executed_2 = breakeven_stop_profit(symbol, starta_direction, breakeven_price_2, add_position_1, start_price_reached=start_price_reached_2, trade_executed = trade_executed_2)
        logger.info(f"保本止盈2{trade_executed_2}，触发状态{start_price_reached_2}：保本价格{breakeven_price_2}")
      if trade_executed_2 and add_position_1 != 0:
        starta_position += add_position_1  # 更新持仓量
        add_position_1 = 0
        trade_executed_1 = True
        logger.info(f"保本止盈2{trade_executed_2}，重置交易量，动态追踪1改{trade_executed_1}")
      if not trade_executed_3 and starta_position != 0:
        logger.info(f"动态追踪3{trade_executed_3}，触发状态{start_price_reached_3}：最佳价格{optimal_price_3}")
        start_price_reached_3, trade_executed_3, optimal_price_3 = dynamic_tracking(symbol, add_direction, add_rate, optimal_price_3, starta_position, start_price_reached=start_price_reached_3, trade_executed = trade_executed_3)
        logger.info(f"动态追踪3{trade_executed_3}，触发状态{start_price_reached_3}：最佳价格{optimal_price_3}")
      if trade_executed_3 and starta_position != 0:
        starta_position = 0
        trade_executed_4 = True
        logger.info(f"动态追踪3{trade_executed_3}，重置交易量,保本止盈4改{trade_executed_4}")
        starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), 1)
      if not trade_executed_4 and breakeven_price_4 != 0:
        logger.info(f"保本止盈4{trade_executed_4}，触发状态{start_price_reached_4}：保本价格{breakeven_price_4}")
        start_price_reached_4, trade_executed_4 = breakeven_stop_profit(symbol, add_direction, breakeven_price_4, starta_position, start_price_reached=start_price_reached_4, trade_executed = trade_executed_4)
        logger.info(f"保本止盈4{trade_executed_4}，触发状态{start_price_reached_4}：保本价格{breakeven_price_4}")
      if trade_executed_4 and starta_position != 0:
        starta_position = 0
        trade_executed_3 = True
        logger.info(f"保本止盈4{trade_executed_4}，重置交易量，动态追踪1改{trade_executed_3}")
        starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), 1)

      # 价格到达触发点
      if (current_price < trigger_price and starta_direction == 'lb') or (current_price > trigger_price and starta_direction == 'ss'):
        starta_cost = round(((starta_position * starta_cost + trigger_price * add_position) / (starta_position + add_position)), 1)  # 更新启动成本
        starta_price = current_price  # 更新启动价格
        breakeven_price_2 = trigger_price # 更新保本价格
        trigger_price = round(starta_price * (1 - add_rate if starta_direction == 'lb' else 1 + add_rate), 1) # 更新追加价格
        profit_price = round(starta_cost * (1 - add_rate if starta_direction == 'ss' else 1 + add_rate), 1)  # 更新止盈价格
        add_position_1 += add_position # 更新交易量
        logger.info(f"更新启动价格：{starta_price}, 启动成本：{starta_cost}")
        logger.info(f"更新追加价格：{trigger_price}, 止盈价格：{profit_price}")
        if (starta_direction == 'lb' and score >= ts_threshold) or \
         (starta_direction == 'ss' and score <= ts_threshold):
          if place_limit_order(symbol, starta_direction, trigger_price, add_position_1, callback):
            trade_executed_1 = True
            trade_executed_2 = True
            starta_position += add_position_1  # 更新持仓量
            add_position_1 = 0
            logger.info(f"成功对冲下单1：{trigger_price}")
        else:
          if (starta_direction == 'lb' and current_price < optimal_price_1) or \
           (starta_direction == 'ss' and current_price > optimal_price_1):
            optimal_price_1 = current_price  # 更新最佳价格
            logger.info(f"更新最佳价格：{optimal_price_1}")

          add_position_1 += add_position
          start_price_reached_1 = False
          start_price_reached_2 = False
          trade_executed_1 = False
          trade_executed_2 = False
          start_price_reached_1,trade_executed_1, optimal_price_1 = dynamic_tracking(symbol, starta_direction, add_rate, optimal_price_1, add_position_1, start_order_price=trigger_price, start_price_reached=start_price_reached_1, trade_executed = trade_executed_1)
          logger.info(f"新对冲单，动态追踪最佳价格{optimal_price_1}，回调率 {add_rate}")
          if trade_executed_1 and add_position_1 != 0:
            starta_position += add_position_1  # 更新持仓量
            add_position_1 = 0
            trade_executed_2 = True
            logger.info(f"动态追踪1{trade_executed_1}，重置交易量,保本止盈2改{trade_executed_2}")
          start_price_reached_2,trade_executed_2 = breakeven_stop_profit(symbol, starta_direction, breakeven_price_2, add_position_1,start_order_price=trigger_price, start_price_reached=start_price_reached_2, trade_executed = trade_executed_2)
          logger.info(f"新对冲单，保本止盈保本价{breakeven_price_2}")
          if trade_executed_2 and add_position_1 != 0:
            starta_position += add_position_1  # 更新持仓量
            add_position_1 = 0
            trade_executed_1 = True
            logger.info(f"保本止盈2{trade_executed_2}，重置交易量，动态追踪1改{trade_executed_1}")

        logger.info(f"新订单已放置在 {trigger_price}，仓位更新为 {starta_position}，平均成本更新为 {starta_cost}，止盈价格更新为 {profit_price}")


      # 检查是否达到止盈点
      if (starta_position != 0 and starta_direction == 'lb' and current_price > profit_price) or (starta_position != 0 and starta_direction == 'ss' and current_price < profit_price):
        profit_position =  starta_position - add_position  # 止盈量
        starta_position = add_position
        if (starta_direction == 'lb' and score <= -ts_threshold) or \
         (starta_direction == 'ss' and score >= ts_threshold):
          if place_limit_order(symbol, starta_direction, profit_price, starta_position, callback):
            starta_position = 0
            trade_executed_3 = True
            trade_executed_4 = True
            logger.info("达到止盈点，评分下单平仓...")
        else:
          if (add_direction == 'lb' and optimal_price_3 == 0) or \
           (add_direction == 'ss' and optimal_price_3 == 0):
            optimal_price_3 = current_price
          elif (add_direction == 'lb' and current_price > optimal_price_3) or \
           (add_direction == 'ss' and current_price < optimal_price_3):
            optimal_price_3 = current_price
          breakeven_price_4 = current_price
          start_price_reached_3 = False
          start_price_reached_4 = False
          start_price_reached_3, trade_executed_3, optimal_price_3 = dynamic_tracking(symbol, add_direction, add_rate, optimal_price_3, starta_position,start_order_price=profit_price, start_price_reached=start_price_reached_3, trade_executed = trade_executed_3)
          if trade_executed_3 and starta_position != 0:
            starta_position = 0
            trade_executed_4 = True
            starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), 1)
          start_price_reached_4, trade_executed_4 = breakeven_stop_profit(symbol, add_direction, breakeven_price_4, starta_position, start_order_price=profit_price, start_price_reached=start_price_reached_4, trade_executed = trade_executed_4)
          if trade_executed_4 and starta_position != 0:
            starta_position = 0
            trade_executed_3 = True
            starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), 1)
          logger.info("达到止盈点，动态保本...")

      status_manager.update_status('trade_executed_1', trade_executed_1)
      status_manager.update_status('trade_executed_2', trade_executed_2)
      status_manager.update_status('trade_executed_3', trade_executed_3)
      status_manager.update_status('trade_executed_4', trade_executed_4)
      status_manager.update_status('start_price_reached_1', start_price_reached_1)
      status_manager.update_status('start_price_reached_2', start_price_reached_2)
      status_manager.update_status('start_price_reached_3', start_price_reached_3)
      status_manager.update_status('start_price_reached_4', start_price_reached_4)
      status_manager.update_status('add_position_1', add_position_1)
      status_manager.update_status('optimal_price_1', optimal_price_1)
      status_manager.update_status('optimal_price_3', optimal_price_3)
      status_manager.update_status('breakeven_price_2', breakeven_price_2)
      status_manager.update_status('breakeven_price_4', breakeven_price_4)
      status_manager.update_status('starta_price', starta_price)
      status_manager.update_status('starta_cost', starta_cost)
      status_manager.update_status('starta_position', starta_position)
      status_manager.update_status('trigger_price', trigger_price)
      status_manager.update_status('profit_price', profit_price)

      logger.info(f"-暂停对冲策略，当前价格：{current_price}, 启动价格：{starta_price}")
      logger.info(f"-启动仓位：{starta_position}, 启动成本：{starta_cost}")
      logger.info(f"-追加价格：{trigger_price}, 止盈价格：{profit_price}")
    except Exception as e:
      logger.error(f"执行trading_strategy时发生错误: {e}")
      traceback.print_exc()
#调用本地函数
def switch_config():
  global current_config
  return 'b' if current_config == 'a' else 'a'

def get_config_file():
  return f'config_{current_config}.txt'

def get_alternate_config_file():
  alternate = switch_config()
  status_manager.update_status('current_config', alternate)
  return f'config_{alternate}.txt'

def execute_config_file(config_file_path):
  config_file = build_file_path(config_file_path)
  try:
      logger.info(f"加载配置{current_config}...")
      with open(config_file, 'r', encoding='utf-8') as file:
          script = file.read()
          exec(script, globals())
  except FileNotFoundError:
      logger.error(f"{config_file} 文件未找到。")
  except SyntaxError as e:
      logger.error(f"{config_file}中的语法错误: {e}")
  except Exception as e:
      logger.error(f"读取{config_file}时发生错误: {e}")

async def fetch_and_update_config():
  global current_config, last_config_update_time
  try:
      # 加载配置和令牌
      config = load_config()
      token = config['github_api_key']
      repo = 'kongji1/go'  # 替换为实际的仓库名称
      file_path = 'go.txt'  # 替换为实际的文件路径

      # 构建API请求
      api_url = f'https://api.github.com/repos/{repo}/commits?path={file_path}'
      headers = {'Authorization': f'token {token}'}

      # 发送请求并解析响应
      response = requests.get(api_url, headers=headers)
      response.raise_for_status()
      commits = response.json()

      if commits:
          last_commit = commits[0]
          last_modified_time_str = last_commit['commit']['committer']['date']

          if last_modified_time_str.endswith('Z'):
              last_modified_time_str = last_modified_time_str[:-1]
          last_modified_time = datetime.fromisoformat(last_modified_time_str)

          # 比较最新提交时间和本地记录的最后更新时间
          if last_config_update_time is None or last_modified_time > last_config_update_time:
              # 更新 last_config_update_time 并继续更新操作
              last_config_update_time = current_time
              last_config_update_time_str = last_config_update_time.isoformat()
              alternate_config_path = get_alternate_config_file()
              alternate_config = build_file_path(alternate_config_path)
              # 从raw URL下载go.txt
              raw_response = requests.get('https://raw.githubusercontent.com/kongji1/go/main/go.txt')
              raw_response.raise_for_status()
              with open(alternate_config, 'w', encoding='utf-8') as file:
                file.write(raw_response.text)
              status_manager.update_status('last_config_update_time', last_config_update_time_str)
              logger.info(f"{alternate_config}已成功更新。尝试执行新配置。")
              execute_config_file(alternate_config)
              current_config = switch_config()
              logger.info(f"现在使用配置文件: {get_config_file()}")
          else:
                  logger.info("文件未更新，无需执行操作。")
      else:
        #logger.info(f"{response.content}")
        logger.error("未能找到更新时间标签。")
  except Exception as e:
      logger.error(f"更新或执行加载配置时出错: {e}")

async def schedule_config_updates():
  while True:
      fetch_and_update_config()
      logger.info(f"暂停8,14400")
      await asyncio.sleep(14400)  # 等待4小时

def get_max_limit():
  try:
    max_limit = max(
        len(kline_data_cache.get(interval, [])),
        len(kline_data_cache.get(interval_ssbb, [])),
        limit,
        limit_test
    )
    logger.info(f"最大 limit calculated: {max_limit}")
    return max_limit
  except Exception as e:
    logger.info(f"get_max_limit出错: {e}")

async def run_periodic_tasks():
  global last_config_update_time, current_time
  last_config_update_time = price_to_datetime(last_config_update_time_str)
  while True:
      try:
          current_time = datetime.now()
          if current_time.minute % 2 == 1:
              await fetch_and_update_config()
          if current_time.minute % 1440 == 2:
              # 在执行任务前，重新加载数据
              max_limit = get_max_limit()
              await cache_kline_data(symbol, [interval, interval_ssbb], max_limit)

              # 创建并执行任务
              logger.info(f"异步开始")
              await rsibest()
              await mfibest()
              await sobest()

      except Exception as e:
          logger.error(f"在运行周期性任务时出现错误: {e}")
      finally:
          logger.info(f"暂停8,{60}")
          await asyncio.sleep(60)



async def main():
    task1 = asyncio.create_task(run_periodic_tasks())  # 创建运行定期任务的任务
    task2 = asyncio.create_task(main_loop())   # 创建主循环任务
    await asyncio.gather(task1, task2)  # 同时运行任务

# 主交易逻辑
async def main_loop():
    global interval, current_price, ssbb, sbsb, last_price_update_time
    start_time = datetime.now()
    last_price_update_time = price_to_datetime(last_price_update_time_str)
    start_price, last_price_update_time = get_current_price(symbol)
    logger.info(f"开始价格: {start_price}")

    intervals = [interval, interval_ssbb]
    await cache_kline_data(symbol, intervals, limit_test) 
    fetch_and_update_active_orders(symbol)  # 加载订单记录
    current_price = start_price
    while True:
        try:
            logger.info("\n******* 正在执行主循环 *******")
            execute_config_file(get_config_file())# 动态变量加载
            get_current_time()
            if current_time - start_time >= timedelta(minutes = max_run_time):
              logger.info("主循环运行超过最大时长，现在停止。")
              break
            await update_kline_data_async(symbol, current_time)
            current_price, last_price_update_time = get_current_price(symbol)
            trading_strategy()
            ssbb, sbsb = calculate_composite_score(current_price, last_order_price, last_s_order_price, stop_loss_limit, take_profit_limit)
          #  logger.info(f"config后sbsb: {sbsb},ssbb: {ssbb}\n")
            if sbsb == 1: 
              current_price, last_price_update_time = get_current_price(symbol)
              order_price, origQty = calculate_next_order_parameters(current_price, leverage)
              order_position = 'lb' if ssbb == 1 else 'ss'
              response = place_limit_order(symbol, order_position, order_price, origQty, 100 * Slippage)
              if response:
                update_order_status(response, order_position)
                logger.info(f"网格系统下单成功")
            current_status()
            logger.info(f"暂停9,{monitoring_interval}")
            await asyncio.sleep(monitoring_interval)  # 等待一分钟
            if current_time.minute % 2 == 1:
              run_powershell_script('112.ps1')
        except  ClientError as error:
            logger.error(f"主循环运行时错误: {error}")
            # 记录完整的堆栈跟踪
            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"主循环运行时1错误: {e}")
            traceback.print_exc()
            # 记录完整的堆栈跟踪
            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
            logger.info(f"暂停10,{monitoring_interval}")
            await asyncio.sleep(monitoring_interval)  # 发生错误时等待一分钟
logger = None
def run_main_loop():
  global logger, client
  while True:
      try:
          logger = setup_logging()
          execute_config_file(get_config_file())
          config = load_config()
          client = setup_um_futures_client()
          get_public_ip_and_latency()
          asyncio.run(main())
          logger.info(f"暂停11,{30}")
          time.sleep(30) 
      except KeyboardInterrupt:
          logger.info("键盘中断检测到。程序将在60秒后重新启动...")
          time.sleep(60) 
          os.execv(sys.executable, ['python'] + sys.argv)
      except Exception as e:
          logger.error(f"程序运行中发生错误: {e}")
          traceback_str = traceback.format_exc()
          logger.error(f"堆栈跟踪: {traceback_str}")
          logger.info(f"暂停12,{30}")
          time.sleep(30) 
          os.execv(sys.executable, ['python'] + sys.argv)


if __name__ == "__main__":
    run_main_loop()

