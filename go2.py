import time  # 用于时间相关操作 3435+duijian+对冲量改相对净持仓
#20250205
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
import ruamel.yaml 
from ruamel.yaml import YAML
import subprocess  # 用于执行外部命令
import math  # 用于数学计算
import pytz  # 导入 pytz 模块
import aiohttp
from aiolimiter import AsyncLimiter
import ssl
import asyncio
import copy  #深度拷贝
import traceback

# 调用保持连接函数
keeplive()

# logging_utils.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os
context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
context.options |= ssl.PROTOCOL_TLS_CLIENT

# 日志过滤器
class IgnoreHttpGetLogsFilter(logging.Filter):
    def filter(self, record):
        # 仅保留不包含特定字符串的日志记录
        return ('GET /logs HTTP/1.1' not in record.getMessage()) and \
               ('OPTIONS * HTTP/1.1' not in record.getMessage())
# 设置日志
def setup_logging():
    # 创建一个logger对象（DEBUG、INFO、WARNING、ERROR、CRITICAL）
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # 设置最低日志级别为DEBUG

    # 创建文件处理器，按天切割日志
    log_file_path = os.path.join(BASE_PATH, 'app.log')  # 确保 BASE_PATH 已定义
    file_handler = TimedRotatingFileHandler(
        log_file_path,
        when='MIDNIGHT',
        interval=1,
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # 文件处理器记录所有DEBUG及以上级别的日志
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    # 根据需要添加或移除过滤器
    file_handler.addFilter(IgnoreHttpGetLogsFilter())
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # 控制台处理器记录INFO及以上级别的日志
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    # 根据需要添加或移除过滤器
    console_handler.addFilter(IgnoreHttpGetLogsFilter())

    # 将处理器添加到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

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

# 异步限流器每分钟不超过120次请求
limiter = AsyncLimiter(120, 60)  

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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
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
    traceback.print_exc()
  # 记录完整的堆栈跟踪
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
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

class CustomUMFutures(UMFutures):
    def get_timestamp(self):
        """重写 get_timestamp 方法，获取 Binance 服务器的时间戳"""
        url = "https://api.binance.com/api/v3/time"
        response = requests.get(url)
        server_time = response.json()['serverTime']
        return server_time
    def sign_request(self, http_method, url_path, payload=None, special=False):
        """重写 sign_request 方法，使用 get_timestamp 获取修正后的时间戳"""
        if payload is None:
            payload = {}
        payload["timestamp"] = self.get_timestamp()  # 使用服务器时间戳
        query_string = self._prepare_params(payload, special)
        payload["signature"] = self._get_sign(query_string)
        return self.send_request(http_method, url_path, payload, special)
    def get_orders(self, **kwargs):
        url_path = "/fapi/v1/openOrders"
        params = {**kwargs}  # 将传入的参数合并到 params 中
        return self.sign_request("GET", url_path, params)

def setup_um_futures_client():
  config = load_config()
  return CustomUMFutures(key=config['binance_api_key'],
                   secret=config['binance_api_secret'])


# 更新status.json
def update_status(key, value):
  status = load_json_file('status.json', {})
  status[key] = value
  try:
    update_json_file('status.json', status)
  except Exception as e:
    logger.error(f"更新status.json时出错: {e}")
    traceback.print_exc()
  # 记录完整的堆栈跟踪
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


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
    self.lock = threading.Lock()  # 添加锁以确保线程安全
    self.init_save_timer()

  @staticmethod
  def build_file_path(file_name):
    return os.path.join(BASE_PATH, file_name)

  def load_status(self):
    try:
      if not os.path.exists(self.file_path):
        if os.path.exists(self.file_path + ".bak"):
          os.rename(self.file_path + ".bak", self.file_path)
      with open(self.file_path, 'r', encoding='utf-8') as file:
        all_statuses = self.yaml.load(file) or {}
        return all_statuses.get(self.trading_pair, {})
    except Exception as e:
      traceback.print_exc()
      # 记录完整的堆栈跟踪
      logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
      if self.logger:
        self.logger.error(f"读取状态文件时出错: {e}")
      return {}

  def get_status(self, key, default=None):
    return self.status.get(key, default)

  def update_status(self, key, value):
    if self.status.get(key) != value:
      self.status[key] = value
      #print(f"更新状态: {key} = {value}")
      self.reset_save_timer()

  def save_status(self):
    with self.lock:  # 确保同一时间只有一个线程执行此块
      backup_file_path = self.file_path + ".bak"
      temp_file_path = self.file_path + ".tmp"

      try:
        # 创建状态文件的备份
        if os.path.exists(self.file_path):
          os.replace(self.file_path, backup_file_path)

        # 读取现有状态或初始化空状态
        all_statuses = {}
        if os.path.exists(backup_file_path):
          with open(backup_file_path, 'r', encoding='utf-8') as backup_file:
            all_statuses = self.yaml.load(backup_file) or {}

        # 更新当前交易对的状态
        all_statuses[self.trading_pair] = self.status

        # 创建一个新的 YAML 实例以避免潜在的线程问题
        yaml_instance = ruamel.yaml.YAML()
        yaml_instance.preserve_quotes = True
        yaml_instance.indent(mapping=2, sequence=4, offset=2)

        # 先将更新写入临时文件
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
          yaml_instance.dump(all_statuses, temp_file)

        # 重命名临时文件为正式文件，确保写入的原子性
        os.replace(temp_file_path, self.file_path)
        if self.logger:
          self.logger.info("状态已保存到文件: " + self.trading_pair)

        # 状态保存成功后，删除备份文件
        if os.path.exists(backup_file_path):
          os.remove(backup_file_path)

      except Exception as e:
          traceback.print_exc()
          # 记录完整的堆栈跟踪
          if self.logger:
              self.logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
              self.logger.error(f"保存状态到文件时出错: {e}")
                # 如果出现错误，尝试从备份恢复
          if os.path.exists(backup_file_path):
              try:
                  os.replace(backup_file_path, self.file_path)
                  if self.logger:
                    self.logger.info("已从备份中恢复到上一个状态文件版本")
              except Exception as recover_e:
                  self.logger.error(f"恢复备份时出错: {recover_e}")
                  self.logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

      finally:
          self.init_save_timer()


  def init_save_timer(self):
    self.save_timer = threading.Timer(self.save_interval, self.save_status)
    self.save_timer.start()

  def reset_save_timer(self):
    if self.save_timer is not None:
      self.save_timer.cancel()
    self.save_timer = threading.Timer(5, self.save_status)
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
    logger.warning(
      f"PowerShell script '{file_name}' does not exist. Skipping execution.")
    return
  powershell_executable = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
  subprocess.run(f"start {powershell_executable} -File {script_path}",
                 check=True,
                 shell=True)
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
  global long_position, short_position, long_cost, short_cost, transaction_fee_rate
  try:
    active_orders = client.get_orders(symbol=symbol, recvWindow=5000)
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
    logger.info("活跃订单及持仓信息已更新")
    load_order_records()  # 更新内存中的订单记录
    response = client.get_position_risk(symbol=symbol)
    long_position = round(float(response[0]["positionAmt"]), dpq)
    long_cost = round(float(response[0]["entryPrice"]), dpp)
    short_position = -round(float(response[1]["positionAmt"]), dpq)
    short_cost = round(float(response[1]["entryPrice"]), dpp)
    logger.info(f"{long_cost}多{long_position}")
    logger.info(f"{short_cost}空{short_position}")
    # makerCommissionRate挂单手续费
    if transaction_fee_rate != float(client.commission_rate(symbol=symbol)['takerCommissionRate']):
      transaction_fee_rate = float(client.commission_rate(symbol=symbol)['takerCommissionRate'])
      status_manager.update_status('transaction_fee_rate', transaction_fee_rate)
    status_manager.update_status('long_position', long_position)
    status_manager.update_status('long_cost', long_cost)
    status_manager.update_status('short_position', short_position)
    status_manager.update_status('short_cost', short_cost)

  except ClientError as error:
    logger.error(f"获取活跃订单时发生错误: {error}")
    logger.error(f"{error.args[0], error.args[1], error.args[2]}")
  except Exception as e:
    logger.error(f"处理活跃订单时出现未预期的错误: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


# 检查并更新订单的成交状态
def check_and_update_order_status(symbol):
  global cached_orders
  updated = False
  for record in cached_orders:
    if record['status'] in ['NEW', 'PARTIALLY_FILLED']:
      try:
        response = client.query_order(symbol=symbol,
                                      orderId=record['order_id'])
        if response:
          new_status = response['status']
          if new_status != record['status']:
            record['status'] = new_status
            updated = True
            logger.info(f"订单 {record['order_id']} 状态更新为 {new_status}"
                        )
      except ClientError as error:
        logger.error(f"查询订单 {record['order_id']} 状态时出错: {error}"
                     )
      except Exception as e:
        logger.error(f"查询订单 {record['order_id']} 状态时出现未预期的错误: {e}"
                     )
        traceback.print_exc()
        # 记录完整的堆栈跟踪
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

  if updated:
    update_json_file('order_records.json', cached_orders)

# client.commission_rate(symbol) 获取某个交易对的佣金率

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
      traceback.print_exc()
      # 记录完整的堆栈跟踪
      logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


# 使用此函数时请谨慎，并确保您理解相关的安全风险
is_paused = False
# 交易和市场相关参数
symbol = 'DOGEUSDT'  #@@ 交易对符号 BTCUSDT ETHUSDT LSKUSDT
symbolp = "DOGEUSDT_PERP"  # 特定交易产品的标识符
side = 'BUY'  # 交易方向，'BUY' 表示买入操作
order_type = 'LIMIT'  # 订单类型，'LIMIT' 表示限价订单
time_in_force = 'GTC'  # 订单时效，'GTC' 表示订单有效直到取消（Good Till Canceled）
profit1 = 0.01  # 利润参数1，用于计算和设置盈利目标
profit2 = 0.002  # 利润参数2，通常设定为小于 profit1 的值
Trailing_take_profit = 0.005  # 追踪止盈比例，例如 0.005 表示追踪止盈为0.5%
Slippage = 0.002  # 滑点，callback(%)=100slippage,
callback = 100 * Slippage  # 回调比例，例如 100 * Slippage 表示回调为100倍滑点

# 量化交易和策略参数
leverage = 1.01  # 杠杆倍数，用于放大交易头寸
interval_5m = "5m"  # K线周期，例如 '5m' 表示5分钟
interval_ssbb_3m = "3m"  # K线周期，例如 '5m' 表示5分钟
interval_4h = "4h" 
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
trading_strategy_enabled = 0  # 追加对冲策略开关

# 订单和仓位管理相关参数

step_size = 0.001  # 订单量调整的步长
max_position_size = 0.1  #@@ 允许的最大持仓量，0.025 0.1
force_reduce = 0  # 是否启用强制减仓，0 表示不启用

# 程序运行和性能参数
max_run_time = 60 * 60 * 24 * 7  # 最大运行时间，以秒为单位
monitoring_interval = 20  # 监测策略条件成立的时间间隔（秒）
order_interval = 60  # 下单操作的时间间隔（秒）
update_interval = 20  # 更新价格的时间间隔（秒）
ltime = 55  # 循环限速时间（秒）
stime = 550  # 交易操作的频率（秒）

# 全局变量和状态维护
current_time = None  # 当前时间，初始设置为 None
rsi = None  # 相对强弱指数（RSI），用于技术分析
mfi = None  # 资金流量指标（MFI），用于分析买卖压力
ema5 = None  # 5周期指数移动平均线（EMA5），用于趋势分析
kline_data_cache = {}  # 存储不同时间间隔K线数据的字典
initial_balance = 100  # 交易账户的初始余额
long_profit, short_profit = 0, 0  # 分别记录多头和空头头寸的利润
cached_orders = []  # 全局变量来存储订单数据
last_known_price = None  # 用于存储最后一次成功获取的价格
last_price_update_time = None  # 用于存储最后一次价格更新的时间
sbsb = 0  # 策略的初始交易开关状态，0 表示关闭

#对冲策略
import re
status_manager = StatusManager('status.yaml', symbol, save_interval=60)
# 从status字典中获取值，并确保它们不是None， 
first_assignment_done  = status_manager.get_status('first_assignment_done ', True) # congfig动态加载执行完成
transaction_fee_rate = status_manager.get_status('transaction_fee_rate', 0.0005)
min_price_step = status_manager.get_status('min_price_step', 0.001)
dpp = len(re.findall(r'\.(\d+?)0*$', f"{min_price_step:.10f}")[0]) if '.' in f"{min_price_step:.10f}" else 0  #decimal_places最小价格步长的小数位数
step_size = status_manager.get_status('step_size', 0.001)  # 订单量调整的步长
min_quantity = status_manager.get_status('min_quantity', 0.01)
min_quantity_u = status_manager.get_status('min_quantity_u', 5)
dpq = len(str(min_quantity).split('.')[1]) if '.' in str(
  min_quantity) else 0  #decimal_places最小交易量的精度
LongRisk = status_manager.get_status('LongRisk', 0.01)  # 多单风控比例示例值
LongRisk = min(LongRisk, 0.75)
ShortRisk = status_manager.get_status('ShortRisk', 0.01) # 空单风控比例示例值
ShortRisk = min(ShortRisk, 0.25)
max_position_size_long = status_manager.get_status('max_position_size_long', 0.1)
max_position_size_short = status_manager.get_status('max_position_size_short', 0.1)
starta_direction = status_manager.get_status('starta_direction', 'lb')
add_rate = status_manager.get_status('add_rate', 0.02)  # 追加仓位的跨度
ts_threshold = status_manager.get_status('ts_threshold', 75)  #分数阈值
current_config = status_manager.get_status('current_config', 'a')
long_position = status_manager.get_status('long_position', 0)
short_position = status_manager.get_status('short_position', 0)
long_cost = status_manager.get_status('long_cost', 0)
short_cost = status_manager.get_status('short_cost', 0)
initial_margin = status_manager.get_status('initial_margin', 265)
floating_margin = status_manager.get_status('floating_margin', 0)
initial_balance = status_manager.get_status('initial_balance', 0)
last_order_price = status_manager.get_status('last_order_price', 0)
last_order_orderId = status_manager.get_status('last_order_orderId', 0)
last_s_order_price = status_manager.get_status('last_s_order_price', 0)
last_s_order_orderId = status_manager.get_status('last_s_order_order', 0)
temp_ssbb = status_manager.get_status('ssbb', 0)
FP = status_manager.get_status('FP', 0.01)
quantity_grid = status_manager.get_status('quantity_grid', min_quantity)  #网格单位交易量
quantity_grid_u = status_manager.get_status('quantity_grid_u', 5)  #网格单位交易量
quantity_grid_rate = status_manager.get_status('quantity_grid_rate', 1)  #网格单位交易量
grid_adjustment_factor = status_manager.get_status('grid_adjustment_factor', 2)  #买卖原始基础上浮动比
quantity = status_manager.get_status('quantity', min_quantity)  #网格最近交易量
quantity_u = status_manager.get_status('quantity_u', 5)  #网格最近交易量
last_order_direction = status_manager.get_status('last_order_direction', 'BUY')

last_price_update_time_str = status_manager.get_status(
  'last_price_update_time', None)
last_config_update_time_str = status_manager.get_status(
  'last_config_update_time', None)

rsi_trigger_low_5m = status_manager.get_status('rsi_trigger_low_5m', 30)
rsi_trigger_high_5m = status_manager.get_status('rsi_trigger_high_5m', 70)
mfi_trigger_low_5m = status_manager.get_status('mfi_trigger_low_5m', 20)
mfi_trigger_high_5m = status_manager.get_status('mfi_trigger_high_5m', 80)
so_trigger_low_5m = status_manager.get_status('so_trigger_low_5m', 20)
so_trigger_high_5m = status_manager.get_status('so_trigger_high_5m', 80)
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

fastperiod_lower_bound = status_manager.get_status('fastperiod_lower_bound',
                                                   12)
fastperiod_upper_bound = status_manager.get_status('fastperiod_upper_bound',
                                                   27)
slowperiod_lower_bound = status_manager.get_status('slowperiod_lower_bound',
                                                   26)
slowperiod_upper_bound = status_manager.get_status('slowperiod_upper_bound',
                                                   53)
signalperiod_lower_bound = status_manager.get_status(
  'signalperiod_lower_bound', 9)
signalperiod_upper_bound = status_manager.get_status(
  'signalperiod_upper_bound', 18)

average_long_cost = status_manager.get_status('average_long_cost', 0)
average_short_cost = status_manager.get_status('average_short_cost', 0)
average_long_position = status_manager.get_status('average_long_position', 0)
average_short_position = status_manager.get_status('average_short_position', 0)

sosold_lower_bound = status_manager.get_status('sosold_lower_bound', 10)
sosold_upper_bound = status_manager.get_status('sosold_upper_bound', 30)
sobought_lower_bound = status_manager.get_status('sobought_lower_bound', 70)
sobought_upper_bound = status_manager.get_status('sobought_upper_bound', 90)

starta_price = status_manager.get_status('starta_price', 0)
add_position = status_manager.get_status('add_position', min_quantity)
add_position_u = status_manager.get_status('add_position_u', 10)
add_position_rate = status_manager.get_status('add_position_rate', 1)
starta_position = status_manager.get_status('starta_position', add_position)
starta_cost = status_manager.get_status('starta_cost', starta_price)
trade_executed_1 = status_manager.get_status('trade_executed_1', False)
trade_executed_2 = status_manager.get_status('trade_executed_2', False)
trade_executed_3 = status_manager.get_status('trade_executed_3', False)
trade_executed_4 = status_manager.get_status('trade_executed_4', False)
start_price_reached_1 = status_manager.get_status('start_price_reached_1',
                                                  False)
start_price_reached_2 = status_manager.get_status('start_price_reached_2',
                                                  False)
start_price_reached_3 = status_manager.get_status('start_price_reached_3',
                                                  False)
start_price_reached_4 = status_manager.get_status('start_price_reached_4',
                                                  False)
add_position_1 = status_manager.get_status('add_position_1', 0)
optimal_price_1 = status_manager.get_status('optimal_price_1', 0)
optimal_price_3 = status_manager.get_status('optimal_price_3', 0)
breakeven_price_2 = status_manager.get_status('breakeven_price_2', 0)
breakeven_price_4 = status_manager.get_status('breakeven_price_4', 0)

continuous_add_count_lb = status_manager.get_status('continuous_add_count_lb', 0)
continuous_add_count_ss = status_manager.get_status('continuous_add_count_ss', 0)
def requests_retry_session(retries=3,
                           backoff_factor=0.3,
                           status_forcelist=(500, 502, 504),
                           session=None):
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    update_time = None
    return update_time


ssbb = temp_ssbb


# 获取公网IP和延迟函数
from datetime import datetime

# 获取公网IP和延迟函数
def get_public_ip_and_latency():
    try:
        # 获取公网IP信息
        response = requests.get('http://ip-api.com/json/', timeout=5)
        data = response.json()
        if data.get('status') == 'success':
            public_ip = f"{data.get('query', 'Unknown')}, {data.get('country', 'Unknown')}, {data.get('city', 'Unknown')}, {data.get('isp', 'Unknown')}"
        else:
            public_ip = 'Unknown'

        # 测试与 Binance 的延迟
        start_time = time.time()
        requests.get('https://api.binance.com/api/v3/ping', timeout=5)
        latency = round((time.time() - start_time) * 1000, 1)
        logger.info(f"Public IP: {public_ip}, Latency: {latency} ms")
        
        # 获取当前的 IP 历史记录
        ip_history = status_manager.get_status('ip_history', [])
        
        # 检查是否需要追加新的 IP
        if not any(entry.get('ip') == public_ip for entry in ip_history):
            ip_entry = {
                'timestamp': datetime.now().isoformat(),
                'ip': public_ip
            }
            ip_history.append(ip_entry)
            status_manager.update_status('ip_history', ip_history)
            logger.info(f"已追加新的 IP 到历史记录: {public_ip}")
        else:
            logger.info("IP 1未变化，不进行更新。")
        
        return public_ip, latency
    except requests.RequestException as e:
        logger.error(f"网络请求错误: {e}")
        return 'Unknown', None
    except Exception as e:
        logger.error(f"保存 IP 时发生错误: {e}")
        traceback.print_exc()
        return 'Unknown', None


# 获取服务器时间函数
def get_binance_server_time():
  try:
    response = requests.get("https://api.binance.com/api/v3/time",
                            timeout=10).json()
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
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
          #临时调整心跳
          last_price_update_time = current_time
          #last_price_update_time_str = last_price_update_time.isoformat()
          #status_manager.update_status('last_price_update_time',
          #                             last_price_update_time_str)
        else:
          # 如果是备用URL，使用Huobi价格
          data = response.json()
          if data['status'] == 'ok' and 'ticks' in data and len(
              data['ticks']) > 0:
            last_known_price = data['ticks'][0]['ask'][0]
            logger.info(f"get_current_price更新价格{last_known_price}")
          #临时调整心跳
          last_price_update_time = current_time
          #last_price_update_time_str = last_price_update_time.isoformat()
          #status_manager.update_status('last_price_update_time',
          #                             last_price_update_time_str)

      return last_known_price, last_price_update_time
    except requests.exceptions.HTTPError as e:
      logger.error(f"HTTP请求错误: {e}")
    except requests.exceptions.ConnectionError as e:
      logger.error(f"网络连接错误: {e}")
    except requests.exceptions.Timeout as e:
      logger.error(f"请求超时: {e}")
    except Exception as e:
      logger.error(f"获取当前价格时出错: {e}")
      traceback.print_exc()
      # 记录完整的堆栈跟踪
      logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

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
    """
    更新持仓成本及仓位数量

    参数:
      price: 成交价格
      quantity: 成交数量
      position_side: 持仓方向，取值 "LONG" 或 "SHORT"
      operation_type: 操作类型，取值 "BUY" 或 "SELL"
      
    功能:
      – 对于多头（LONG）：BUY操作采用加权平均计算新成本，SELL操作减少仓位（若全部卖出则归零）
      – 对于空头（SHORT）：SELL操作采用加权平均计算新成本，BUY操作减少仓位（若全部买回则归零）
      – 更新全局变量 floating_margin（浮动保证金）
    """
    global long_position, long_cost, short_position, short_cost, floating_margin, initial_balance, current_price

    try:
        # 转换参数类型并统一大写比较
        price = float(price)
        quantity = float(quantity)
        op_type = operation_type.upper()
        pos_side = position_side.upper()

        if pos_side == "LONG":
            if op_type == "BUY":
                # 多头加仓：更新加权平均成本
                total_cost = long_cost * long_position + price * quantity
                long_position += quantity
                long_cost = total_cost / long_position if long_position > 0 else 0
            elif op_type == "SELL":
                # 多头平仓：若卖出数量大于或等于当前仓位，则清零；否则仅减少仓位数量（成本保持不变）
                if quantity >= long_position:
                    long_position = 0
                    long_cost = 0
                else:
                    long_position -= quantity
            else:
                logger.error(f"无效的操作类型 {operation_type} (LONG)")
        elif pos_side == "SHORT":
            if op_type == "SELL":
                # 空头加仓：更新加权平均成本
                total_cost = short_cost * short_position + price * quantity
                short_position += quantity
                short_cost = total_cost / short_position if short_position > 0 else 0
            elif op_type == "BUY":
                # 空头平仓：若买回数量大于或等于当前仓位，则清零；否则仅减少仓位数量（成本保持不变）
                if quantity >= short_position:
                    short_position = 0
                    short_cost = 0
                else:
                    short_position -= quantity
            else:
                logger.error(f"无效的操作类型 {operation_type} (SHORT)")
        else:
            logger.error(f"无效的持仓方向: {position_side}")

        # 根据最新市场价格更新浮动保证金
        floating_margin = initial_balance + (current_price - long_cost) * long_position + (short_cost - current_price) * short_position

        logger.info(f"更新后多头: 数量={long_position}, 成本={long_cost}; "
                    f"空头: 数量={short_position}, 成本={short_cost}; "
                    f"浮动保证金={floating_margin}")

        # 将最新的持仓、成本及浮动保证金保存到状态管理器中
        status_manager.update_status('long_position', round(long_position, dpq))
        status_manager.update_status('short_position', round(short_position, dpq))
        status_manager.update_status('long_cost', round(long_cost, dpp))
        status_manager.update_status('short_cost', round(short_cost, dpp))
        status_manager.update_status('floating_margin', round(floating_margin, dpp))

    except Exception as e:
        logger.error(f"更新持仓成本时出错: {e}")
        logger.debug(traceback.format_exc())

# 获取当前仓位状态
def current_status():
  global net_cost, starta_cost, starta_direction, long_position, short_position, long_cost, short_cost
  try:
    # 构建最近订单信息
    last_order_info = []
    logger.info("\n***** 最近交易与仓位 *****")
    if last_order_price and last_order_price > 0:
      if long_cost >= 0:
          pass  # 不执行任何动作
      else:
          response = client.get_position_risk(symbol=symbol)
          long_position = round(float(response[0]["positionAmt"]), dpq)
          long_cost = round(float(response[0]["entryPrice"]), dpp)
      last_order_info.append(f"-最近买单: {round(last_order_price, dpp)}")

    if last_s_order_price and last_s_order_price > 0:
      if short_cost >= 0:
          pass  # 不执行任何动作
      else:
          response = client.get_position_risk(symbol=symbol)
          short_position = round(float(response[1]["positionAmt"]), dpq)
          short_cost = round(float(response[1]["entryPrice"]), dpp)
      last_order_info.append(f"最近卖单: {round(last_s_order_price, dpp)}")

      status_manager.update_status('long_cost', round(long_cost, dpp))
      status_manager.update_status('long_position', round(long_position, dpq))
      status_manager.update_status('short_cost', round(short_cost, dpp))
      status_manager.update_status('short_position',
                                   round(short_position, dpq))
    last_order_info = ", ".join(last_order_info) or "最近订单: None"

    # 构建成本和持仓量信息 
    cost_info = f"-多头成本: {round(long_cost, dpp)}, 空头成本:{round(short_cost, dpp)}"
    position_info = f"-多头持仓量: {round(long_position, dpq)}, 空头持仓量: {round(short_position, dpq)}"

    # 计算净持仓量
    net_position = max(abs(long_position - short_position), quantity_grid)
    total_profit_loss = (current_price - long_cost) * long_position + (short_cost - current_price) * short_position
    # 计算净成本
    net_cost = round(current_price - total_profit_loss / net_position if long_position >= short_position else current_price + total_profit_loss / net_position, dpp)

    starta_direction_temp = "lb" if long_position >= short_position else "ss"
    if starta_position > add_position:
      starta_cost = round(long_cost * (1 + add_rate) ** 2, dpp) if starta_direction_temp == "lb" else round(short_cost * (1 - add_rate) ** 2, dpp)
    if starta_direction != starta_direction_temp:
      starta_direction = starta_direction_temp
      logger.info(f"更新对冲方向: {starta_direction}")
    net_info = f"-净持仓: {'多' if long_position >= short_position else '空'}{round(net_position, dpq)}, 成本: {round(net_cost, dpp)}"
    # 最近side和余额
    side_and_balance = f"-最近side: {'l' if last_order_direction == 'BUY' else 's' if last_order_direction == 'SELL' else 'None'}:{average_long_cost if last_order_direction == 'BUY' else average_short_cost:.{dpp}f}/{average_long_position if last_order_direction == 'BUY' else average_short_position:.{dpq}f}, 余额: {round(floating_margin, dpp)}" if floating_margin is not None else "None"
    logger.info(side_and_balance)
    logger.info(last_order_info)
    logger.info(cost_info)
    logger.info(position_info)
    logger.info(net_info)
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
    traceback.print_exc()
  # 记录完整的堆栈跟踪
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


def calculate_limit_for_interval(interval, days):
  interval_seconds = {
    '1m': 60,
    '3m': 180,
    '5m': 300,
    '15m': 900,
    '30m': 1800,
    '1h': 3600,
    '2h': 7200,
    '4h': 14400,
    '6h': 21600,
    '8h': 28800,
    '12h': 43200,
    '1d': 86400,
    '3d': 259200,
    '1w': 604800
  }

  seconds_per_day = 86400  # 一天有86400秒
  interval_in_seconds = interval_seconds.get(interval)

  if not interval_in_seconds:
    raise ValueError(f"未知的K线间隔: {interval}")

  data_points_per_day = seconds_per_day / interval_in_seconds
  return max(int(data_points_per_day * days), limit_test)  # 指定天数的数据点数


def is_force_update_time(update_interval_hours):
  current_hour = datetime.now().hour
  return current_hour % update_interval_hours == 0


from dateutil.parser import isoparse


def calculate_needed_klines(elapsed_time, interval):
  # 将间隔字符串转换为秒
  interval_seconds = {
    '1m': 60,
    '3m': 180,
    '5m': 300,
    '15m': 900,
    '30m': 1800,
    '1h': 3600,
    '2h': 7200,
    '4h': 14400,
    '6h': 21600,
    '8h': 28800,
    '12h': 43200,
    '1d': 86400,
    '3d': 259200,
    '1w': 604800
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
    # 验证 interval 格式
    if not isinstance(interval, str) or not interval.endswith(('m', 'h', 'd', 'w')):
      raise ValueError(f"未知的K线间隔: {interval}")
    # 确保字典中存在该键，如果不存在则初始化
    if interval not in kline_data_cache:
        kline_data_cache[interval] = []
        logger.info(f"初始化K线数据缓存: {interval}")
    current_time = datetime.now()
    last_update_time = last_price_update_time_dict.get(interval)
    if is_force_update_time(6) or first_fetch or (interval
                                                  not in kline_data_cache):
      new_limit = calculate_limit_for_interval(interval, 2)

    else:
      if not first_fetch and interval in kline_data_cache:
        if last_update_time:
          elapsed_time = (current_time - last_update_time).total_seconds()
          new_limit = calculate_needed_klines(elapsed_time, interval)
          if new_limit < 1:
            logger.info(f"{interval}K已更新")
            return kline_data_cache[interval]
        else:
          new_limit = limit
      else:
        new_limit = limit

    async with limiter, aiohttp.ClientSession() as session:
      if new_limit < limit_test:
        new_limit = limit_test
      url = f"https://api.binance.com/api/v1/klines?symbol={symbol}&interval={interval}&limit={new_limit}"
      try:
        async with session.get(url) as response:
          if response.status == 200:
            new_data = await response.json()
            if new_data:
              #print(f"{new_data}")
              # 插入数据到本地数据库
              bulk_insert_to_database(new_data, symbol, interval)
              last_known_price = float(new_data[-1][4])
              logger.info(f"k{interval}/{new_limit}更新价格{last_known_price}")
              current_time = datetime.now()
              last_price_update_time_dict[interval] = current_time  # 更新特定间隔的时间
              #临时调整心跳
              #last_price_update_time = current_time
              #last_price_update_time_string = last_price_update_time.isoformat(
              #)
              #status_manager.update_status('last_price_update_time',
              #                             last_price_update_time_string)

              if interval in kline_data_cache:
                existing_data = kline_data_cache[interval]
                merged_data = merge_kline_data(existing_data, new_data)
                kline_data_cache[interval] = merged_data
              else:
                kline_data_cache[interval] = new_data
          # 从本地数据库读取数据
          #  kline_data_cache[interval] = read_recent_kline_data(symbol, interval, new_limit)
          #  print(f"已读取{new_limit}")
          else:
            logger.error(f"请求失败：{response.status}")
      except Exception as e:
        logger.error(f"获取K线数据时出错: {e}")

    return kline_data_cache[interval]
  except Exception as e:
    logger.error(f"get_kline_data_async出错: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return kline_data_cache.get(interval, None)


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
    kline_data_cache[interval] = await get_kline_data_async(
      symbol, interval, limit, first_fetch)


# 更新K线数据的函数
async def update_kline_data_async(symbol, current_time):
    try:
        intervals_to_update = []
        interval_mapping = {
            '1m': lambda dt: True,  # 每分钟更新
            '3m': lambda dt: dt.minute % 3 == 2,
            '5m': lambda dt: dt.minute % 5 == 4,
            '15m': lambda dt: dt.minute % 15 == 14,
            '30m': lambda dt: dt.minute % 30 == 29,
            '1h': lambda dt: dt.minute == 59,
            '2h': lambda dt: dt.minute == 59 and dt.hour % 2 == 1,
            '4h': lambda dt: dt.minute == 59 and dt.hour % 4 == 3,
            '6h': lambda dt: dt.minute == 59 and dt.hour % 6 == 5,
            '8h': lambda dt: dt.minute == 59 and dt.hour % 8 == 7,
            '12h': lambda dt: dt.minute == 59 and dt.hour % 12 == 11,
            '1d': lambda dt: dt.minute == 59 and dt.hour == 23,
            '3d': lambda dt: dt.minute == 59 and dt.hour == 23 and (dt.day - 1) % 3 == 2,
            '1w': lambda dt: dt.minute == 59 and dt.hour == 23 and dt.weekday() == 6,
        }
        if current_time.second < 21:
          # 根据当前时间和间隔映射，确定需要更新的间隔
          for interval, check_fn in interval_mapping.items():
              if check_fn(current_time):
                  intervals_to_update.append(interval)

          # 异步更新每个需要更新的间隔的数据
          if intervals_to_update:
              await cache_kline_data(symbol, intervals_to_update, limit)  # 一次性处理所有需要更新的间隔
              for interval in intervals_to_update:
                  logger.debug(f"已更新: {interval} {len(kline_data_cache[interval])}\n")
    except Exception as e:
        logger.error(f"异步更新K线数据时出错: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

import sqlite3

def initialize_database():
    connection = sqlite3.connect(os.path.join(BASE_PATH, 'kline_data.db'))
    cursor = connection.cursor()

    # 创建一个新的表格，包含交易对和时间间隔
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS kline_data (
        symbol TEXT,
        interval TEXT,
        timestamp INTEGER PRIMARY KEY,
        open TEXT,
        high TEXT,
        low TEXT,
        close TEXT,
        volume TEXT,
        close_time INTEGER,
        quote_asset_volume TEXT,
        number_of_trades INTEGER,
        taker_buy_base_asset_volume TEXT,
        taker_buy_quote_asset_volume TEXT,
        extra_column TEXT
    )''')
    connection.commit()
    connection.close()

# 初始化数据库
initialize_database()

def bulk_insert_to_database(data, symbol, interval):
    connection = sqlite3.connect(os.path.join(BASE_PATH, 'kline_data.db'))
    cursor = connection.cursor()

    insert_query = '''
    INSERT OR IGNORE INTO kline_data (symbol, interval, timestamp, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, extra_column)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    # 准备数据
    prepared_data = [(symbol, interval) + tuple(d) for d in data]

    cursor.executemany(insert_query, prepared_data)
    connection.commit()
    connection.close()

def read_recent_kline_data(symbol, interval, limit=100):
    connection = sqlite3.connect(os.path.join(BASE_PATH, 'kline_data.db'))
    cursor = connection.cursor()

    query = '''
    SELECT timestamp, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume
    FROM kline_data
    WHERE symbol = ? AND interval = ?
    ORDER BY timestamp DESC
    LIMIT ?'''

    cursor.execute(query, (symbol, interval, limit))
    rows = cursor.fetchall()
    connection.close()
    # 将元组列表转换为列表的列表
    data_as_lists = [list(row) for row in rows]
    return data_as_lists

#领先指标 (Leading Indicators)：RSI SO CCI Williams%R
#随机震荡指数stochastic_oscillator
def c_so(interval, k_window=14, d_window=3):
  global kline_data_cache  #异步获取K线的数据
  try:
    data = copy.deepcopy(kline_data_cache[interval])
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None, None


async def backtest_so(interval,
                      overbought,
                      oversold,
                      k_window=14,
                      d_window=3,
                      initial_capital=10000):
  global kline_data_cache
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    percent_k, percent_d = c_so(interval, k_window, d_window)
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None, None, None

async def sobest(interval):
  global so_trigger_low_5m, so_trigger_high_5m, sosold_lower_bound, sosold_upper_bound, sobought_lower_bound, sobought_upper_bound
  try:
    best_return = -np.inf
    oversold_range = range(max(1, sosold_lower_bound),
                           min(100, sosold_upper_bound))
    overbought_range = range(max(1, sobought_lower_bound),
                             min(100, sobought_upper_bound))
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
        overbought_range = range(max(0, best_overbought - 10),
                                 min(100, best_overbought + 10))
        overbought_adjusted = True

      if best_oversold in [oversold_range.start, oversold_range.stop - 1]:
        oversold_range = range(max(0, best_oversold - 10),
                               min(100, best_oversold + 10))
        oversold_adjusted = True

      if not overbought_adjusted and not oversold_adjusted:
        break  # 如果没有调整，则提前结束迭代

      iterations += 1
    status_manager.update_status('sosold_lower_bound', sosold_lower_bound)
    status_manager.update_status('sosold_upper_bound', sosold_upper_bound)
    status_manager.update_status('sobought_lower_bound', sobought_lower_bound)
    status_manager.update_status('sobought_upper_bound', sobought_upper_bound)
    logger.info(f"更新后的so触发点：超卖阈值 {best_oversold}, 超买阈值 {best_overbought}")
    return best_overbought, best_oversold, best_return
  except Exception as e:
    logger.error(f"sobest出错: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return 23, 84, None
    


# RSI计算（领先指标 (Leading Indicators)）
def c_rsi(interval, length):
  global kline_data_cache  #异步获取K线的数据
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    current_price, _ = get_current_price(symbol)
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
    logger.error(f"RSI计算出错: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None


# 回测函数，根据RSI超卖阈值进行交易
async def backtest_rsi(interval,
                       oversold_range,
                       overbought_range,
                       length=14,
                       initial_capital=10000):
  #param interval: 时间间隔标识符。
  #param oversold_range: 超卖阈值范围。
  #param overbought_range: 超买阈值范围。
  #param length: RSI计算的长度。
  #param initial_capital: 初始资本。
  #return: 最佳超买、超卖阈值和相应的回报率。
  global kline_data_cache  #异步获取K线的数据
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    rsi = c_rsi(interval, length)
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None, None, None


async def rsibest(interval):
  global rsi_trigger_low_5m, rsi_trigger_high_5m, rsisold_lower_bound, rsisold_upper_bound, rsibought_lower_bound, rsibought_upper_bound

  try:
    found_optimal = False
    expanded = False  # 标记是否已经扩展过范围
    iterations = 0  # 添加迭代次数计数器
    max_iterations = 10  # 设置最大迭代次数
    testrsi = 0
    while not found_optimal and iterations < max_iterations:
      oversold_range = range(rsisold_lower_bound, rsisold_upper_bound)
      overbought_range = range(rsibought_lower_bound, rsibought_upper_bound)

      best_oversold, best_overbought, best_return = await backtest_rsi(
        interval, oversold_range, overbought_range)
      logger.info(f"RSI测试范围：超卖 {oversold_range}, 超买 {overbought_range}")
      logger.info(
        f"最佳超卖阈值: {best_oversold}, 最佳超买阈值: {best_overbought}, 最佳回报率: {best_return:.2%}"
      )
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
          status_manager.update_status('rsisold_lower_bound',
                                       rsisold_lower_bound)
          status_manager.update_status('rsisold_upper_bound',
                                       rsisold_upper_bound)
          status_manager.update_status('rsibought_lower_bound',
                                       rsibought_lower_bound)
          status_manager.update_status('rsibought_upper_bound',
                                       rsibought_upper_bound)
          logger.info(
            f"更新后的RSI触发点：超卖阈值 {rsi_trigger_low_5m}, 超买阈值 {rsi_trigger_high_5m}")
          found_optimal = True
      else:
        logger.info(
          f"维持原RSI触发点：超卖阈值 {rsi_trigger_low_5m}, 超买阈值 {rsi_trigger_high_5m}")
        break  # 如果没有找到更优的阈值组合，跳出循环

      iterations += 1  # 增加迭代计数

    if iterations >= max_iterations:
      logger.info(
        "达到最大迭代次数，结束搜索, 维持原RSI触发点：超卖阈值 {rsi_trigger_low_5m}, 超买阈值 {rsi_trigger_high_5m}"
      )

      # 可以在这里返回最终确定的最优阈值和相关信息
    return best_oversold, best_overbought, best_return

  except Exception as e:
    logger.error(f"rsibest 函数运行时出现错误: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    # 可以选择返回默认值或者特定的错误指示
    return 40, 70, None


# MFI计算
def c_mfi(interval, length):
  global kline_data_cache  #异步获取K线的数据
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    #data = kline_data_cache[interval]
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None


async def backtest_mfi(interval,
                       mfi_low_range,
                       mfi_high_range,
                       mfi_length=14,
                       initial_capital=10000):
  #param interval: 时间间隔标识符。
  ##param mfi_low_range: MFI超卖阈值范围。
  #param mfi_high_range: MFI超买阈值范围。
  #param mfi_length: MFI计算的长度。
  #param initial_capital: 初始资本。
  #return: 最佳MFI超卖、超买阈值和相应的回报率。
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    mfi = c_mfi(interval, mfi_length)
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None, None, None


async def mfibest(interval):
  global mfi_low_lower_bound, mfi_low_upper_bound, mfi_high_lower_bound, mfi_high_upper_bound
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

      best_mfi_low, best_mfi_high, best_return = await backtest_mfi(
        interval, mfi_low_range, mfi_high_range)
      logger.info(f"MFI测试范围：低 {mfi_low_range}, 高 {mfi_high_range}")
      logger.info(
        f"最佳MFI超卖阈值: {best_mfi_low}, 最佳MFI超买阈值: {best_mfi_high}, 最佳回报率: {best_return:.2%}"
      )
      testmfi += 1
      logger.info(f"testmacd {testmfi}")
      if best_mfi_low is not None and best_mfi_high is not None:
        # 如果已经扩展过范围，并且再次触及边界值，恢复初始范围并停止扩展

        if expanded and (best_mfi_low in [0, 100]
                         or best_mfi_high in [0, 100]):
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
            status_manager.update_status('mfi_low_lower_bound',
                                         mfi_low_lower_bound)
            status_manager.update_status('mfi_low_upper_bound',
                                         mfi_low_upper_bound)
            status_manager.update_status('mfi_high_lower_bound',
                                         mfi_high_lower_bound)
            status_manager.update_status('mfi_high_upper_bound',
                                         mfi_high_upper_bound)
            logger.info(
              f"更新后的MFI触发点：超卖阈值 {best_mfi_low}, 超买阈值 {best_mfi_high}")
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return 38, 77, None


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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
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
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return pd.Series(), pd.Series()


async def backtest_macd(interval,
                        fastperiod_range,
                        slowperiod_range,
                        signalperiod_range,
                        initial_capital=10000):
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
    logger.info(
      f"找到的最佳周期：fastperiod={best_params[0]}, slowperiod={best_params[1]}, signalperiod={best_params[2]}"
    )
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
  signalperiod_range = range(signalperiod_lower_bound,
                             signalperiod_upper_bound)
  testmacd = 0
  while True:
    # 执行MACD回测
    current_best_params, current_best_return = await backtest_macd(
      interval_5m, fastperiod_range, slowperiod_range, signalperiod_range)
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
      signalperiod_range = range(signalperiod_lower_bound,
                                 signalperiod_upper_bound)
      logger.info(
        f"MACD测试范围：快 {fastperiod_range}, 慢 {slowperiod_range}, signal {signalperiod_range}"
      )
      logger.info(f"找到更好的参数组合：{best_params}，回报率：{best_return:.2%}，更新搜索范围。")
    else:
      # 没有找到更好的组合，更新全局参数并退出循环
      fastperiod, slowperiod, signalperiod = best_params
      status_manager.update_status('fastperiod', fastperiod)
      status_manager.update_status('slowperiod', slowperiod)
      status_manager.update_status('signalperiod', signalperiod)
      status_manager.update_status('fastperiod_lower_bound',
                                   fastperiod_range.start)
      status_manager.update_status('fastperiod_upper_bound',
                                   fastperiod_range.stop - 1)
      status_manager.update_status('slowperiod_lower_bound',
                                   slowperiod_range.start)
      status_manager.update_status('slowperiod_upper_bound',
                                   slowperiod_range.stop - 1)
      status_manager.update_status('signalperiod_lower_bound',
                                   signalperiod_range.start)
      status_manager.update_status('signalperiod_upper_bound',
                                   signalperiod_range.stop - 1)
      logger.info(
        f"未找到更好的MACD参数组合，结束搜索。最终确定的MACD参数：{best_params}，回报率：{best_return:.2%}")
      break

  return best_params


# 支撑线计算
def c_support_resistance(interval):
  global kline_data_cache
  try:
    data = copy.deepcopy(kline_data_cache[interval])
    if current_price is not None:
      data[-1][4] = current_price
    df = pd.DataFrame(
      data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)
    pivot_point = (df['high'] + df['low'] + df['close']) / 3
    resistance = (2 * pivot_point) - df['low']
    support = (2 * pivot_point) - df['high']
    return pivot_point.iloc[-1], support.iloc[-1], resistance.iloc[-1]
  except Exception as e:
    logger.error(f"在c_support_resistance中发生异常：{e}")
    traceback.print_exc()
      # 记录完整的堆栈跟踪
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None, None, None


# 计算ATR
def c_atr(interval, period=14):
  global kline_data_cache
  try:
    kline_data = copy.deepcopy(kline_data_cache[interval])
    if current_price is not None:
      kline_data[-1][4] = current_price
    df = pd.DataFrame(
      kline_data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)
    tr = pd.concat([
      df['high'] - df['low'],
      abs(df['high'] - df['close'].shift()),
      abs(df['low'] - df['close'].shift())
    ],
                   axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr.iloc[-1]
  except Exception as e:
    logger.error(f"在c_atr中发生异常：{e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return None

# =============================================================================
# RMI Trend Sniper 及 ssbb/sbsb 相关逻辑
# =============================================================================

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
      rsi = c_rsi(interval, 14)
      mfi = c_mfi(interval, 14)
      # 设置RSI和MFI的权重
      rsi_weight = 0.4
      mfi_weight = 0.6

      # 计算加权平均值
      rsi_mfi_avg = (rsi * rsi_weight + mfi * mfi_weight) / (rsi_weight +
                                                             mfi_weight)
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
        elif rsi_mfi_avg.iloc[
            i - 1] < pmom and rsi_mfi_avg.iloc[i] > pmom and rsi_mfi_avg.iloc[
              i] > nmom and ema5.pct_change().iloc[i] > 0:
          v_ssbb_series.iloc[i] = 1
          v_sbsb_series.iloc[i] = 1
          v_ssbb_count += 1
        else:
          v_ssbb_series.iloc[i] = v_ssbb_series.iloc[i - 1]  # 继承前一个值
          v_sbsb_series.iloc[i] = 0
          v_sbsb_count += 1

      return v_ssbb_series, v_sbsb_series
    else:
      return pd.Series(), pd.Series()
  except Exception as e:
    logger.error(f"计算v_ssbb时出错: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    return pd.Series(), pd.Series()


score_cache = {'last_score': None, 'last_calc_period': None}


def calculate_score(conditions_enabled):
  global temp_ssbb, score_cache
  current_time = datetime.now()
  # 计算当前时间所在的x分钟周期
  current_period = (current_time.hour * 60 + current_time.minute) // 1

  # 检查是否在新的一分钟内第一次调用
  if score_cache['last_calc_period'] is not None and score_cache[
      'last_calc_period'] == current_period:
    logger.info(f"{current_period}已调用，返回{score_cache['last_score']}")
    return score_cache['last_score']

  data = kline_data_cache[interval_5m]
  if not data:
    logger.error("无K线数据")
    return temp_ssbb, 0  # 返回当前的 ssbb 和 sbsb=0
  if current_price is not None:
    data[-1][4] = current_price
  # 计算技术指标
  rsi = c_rsi(interval_5m, 14)
  mfi = c_mfi(interval_5m, 14)
  ema5 = c_ema(interval_5m, 5)
  macd, signal = c_macd(interval_ssbb_3m, fastperiod, slowperiod, signalperiod)
  percent_k, percent_d = c_so(interval_5m, 14, 3)
  v_ssbb, v_sbsb = c_ssbb(interval_ssbb_3m)
  score = 0
  current_order_direction = 'BUY' if v_ssbb.iloc[-1] == 1 else 'SELL'
  conditions = [
    # ssbb 和 sbsb 逻辑
    # 领先指标 RSI MFI Stochastic Oscillator ssbb
    (conditions_enabled['technical_indicators'],
     rsi.iloc[-1] < rsi_trigger_low_5m, 25,
     f"RSI {rsi.iloc[-1]:.2f} 低于 {rsi_trigger_low_5m}，加25分"),
    (conditions_enabled['technical_indicators'],
     rsi.iloc[-1] > rsi_trigger_high_5m, -25,
     f"RSI {rsi.iloc[-1]:.2f} 超过 {rsi_trigger_high_5m}，减25分"),
    (conditions_enabled['technical_indicators'],
     mfi.iloc[-1] < mfi_trigger_low_5m, 50,
     f"MFI {mfi.iloc[-1]:.2f} 低于 {mfi_trigger_low_5m}，加50分"),
    (conditions_enabled['technical_indicators'],
     mfi.iloc[-1] > mfi_trigger_high_5m, -50,
     f"MFI {mfi.iloc[-1]:.2f} 超过 {mfi_trigger_high_5m}，减50分"),
    (conditions_enabled['technical_indicators'],
     percent_k.iloc[-1] > percent_d.iloc[-1]
     and percent_k.iloc[-1] < so_trigger_high_5m, 25,
     f"%K {percent_k.iloc[-1]:.2f} 大于%D {percent_d.iloc[-1]:.2f}，且未超买，加25分"),
    (conditions_enabled['technical_indicators'],
     percent_k.iloc[-1] < percent_d.iloc[-1]
     and percent_k.iloc[-1] > so_trigger_low_5m, -25,
     f"%K {percent_k.iloc[-1]:.2f} 小于%D {percent_d.iloc[-1]:.2f}，且未超卖，减25分"),
    (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] == 1
     and v_ssbb.iloc[-1] == 1, 50, "ssbb逻辑，加50分"),
    (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] == 1
     and v_ssbb.iloc[-1] == 0, -50, "ssbb逻辑，减50分"),
    (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] != 1
     and v_ssbb.iloc[-1] == 1, 25, "ssbb逻辑，加25分"),
    (conditions_enabled['ssbb_logic'], v_sbsb.iloc[-1] != 1
     and v_ssbb.iloc[-1] == 0, -25, "ssbb逻辑，减25分"),
    # 价格变化逻辑
    (conditions_enabled['price_change'], last_order_price != 0
     and current_price < last_order_price * (1 - 3 * FP), 25, "同向波动超3FP，加25分"),
    (conditions_enabled['price_change'], last_s_order_price != 0
     and current_price > last_s_order_price * (1 + 3 * FP), -25,
     "同向波动超3FP，减25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction and last_order_price != 0
     and average_short_cost != 0
     and current_price < average_short_cost * (1 - 3 * FP)
     and current_price < last_order_price * (1 - 3 * FP), 25,
     "反向波动获利超3FP，加25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction
     and last_s_order_price != 0 and average_long_cost != 0
     and current_price > average_long_cost * (1 + 3 * FP)
     and current_price > last_s_order_price * (1 + 3 * FP), -25,
     "反向波动获利超3FP，减25分"),
    # 价格变化逻辑 变化翻2倍
    (conditions_enabled['price_change'], last_order_price != 0
     and current_price < last_order_price * (1 - 9 * FP), 25, "同向波动超9FP，加25分"),
    (conditions_enabled['price_change'], last_s_order_price != 0
     and current_price > last_s_order_price * (1 + 9 * FP), -25,
     "同向波动超9FP，减25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction and last_order_price != 0
     and average_short_cost != 0
     and current_price < average_short_cost * (1 - 9 * FP)
     and current_price < last_order_price * (1 - 9 * FP), 25,
     "反向波动获利超9FP，加25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction
     and last_s_order_price != 0 and average_long_cost != 0
     and current_price > average_long_cost * (1 + 9 * FP)
     and current_price > last_s_order_price * (1 + 9 * FP), -25,
     "反向波动获利超9FP，减25分"),
    # 价格变化逻辑 变化翻3倍
    (conditions_enabled['price_change'], last_order_price != 0
     and current_price < last_order_price * (1 - 21 * FP), 25, "同向波动超21FP，加25分"
     ),
    (conditions_enabled['price_change'], last_s_order_price != 0
     and current_price > last_s_order_price * (1 + 21 * FP), -25,
     "同向波动超21FP，减25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction and last_order_price != 0
     and average_short_cost != 0
     and current_price < average_short_cost * (1 - 21 * FP)
     and current_price < last_order_price * (1 - 21 * FP), 25,
     "反向波动获利超21FP，加25分"),
    (conditions_enabled['price_change'],
     last_order_direction != current_order_direction
     and last_s_order_price != 0 and average_long_cost != 0
     and current_price > average_long_cost * (1 + 21 * FP)
     and current_price > last_s_order_price * (1 + 21 * FP), -25,
     "反向波动获利超21FP，减25分"),
    # 成本和价格逻辑
    (conditions_enabled['cost_price_logic'], long_cost != 0
     and current_price > long_cost * (1 + 3 * FP)
     and current_price > last_s_order_price * (1 + 3 * FP), -25,
     "获利3FP强制卖出逻辑，减25分"),
    (conditions_enabled['cost_price_logic'], short_cost != 0
     and current_price < short_cost * (1 - 3 * FP)
     and current_price < last_order_price * (1 - 3 * FP), 25,
     "获利3FP强制买入逻辑，加25分"),

    # 滞后指标 MACD
    (conditions_enabled['macd_signal'], macd.iloc[-1] > signal.iloc[-1]
     and macd.iloc[-2] < signal.iloc[-2], 50,
     f"MACD {macd.iloc[-1]:.2f} 上穿信号线{signal.iloc[-1]:.2f}，加50分"),
    (conditions_enabled['macd_signal'], macd.iloc[-1] > signal.iloc[-1]
     and macd.iloc[-2] > signal.iloc[-2], 25,
     f"MACD {macd.iloc[-1]:.2f} 高于信号线{signal.iloc[-1]:.2f}，加25分"),
    (conditions_enabled['macd_signal'], macd.iloc[-1] < signal.iloc[-1]
     and macd.iloc[-2] > signal.iloc[-2], -50,
     f"MACD {macd.iloc[-1]:.2f} 下穿信号线{signal.iloc[-1]:.2f}，减50分"),
    (conditions_enabled['macd_signal'], macd.iloc[-1] < signal.iloc[-1]
     and macd.iloc[-2] < signal.iloc[-2], -25,
     f"MACD {macd.iloc[-1]:.2f} 低于信号线{signal.iloc[-1]:.2f}，减25分")
  ]
  # 遍历所有条件，并根据启用状态计算得分
  logger.info("\n****** 评分系统 ******")  # 在开始之前添加一个换行符
  last_price_update_time_string = last_price_update_time.isoformat()
  status_manager.update_status('last_price_update_time',last_price_update_time_string)
  for is_enabled, condition, score_value, log_message in conditions:
    if is_enabled and condition:
      score += score_value
      logger.info(f" -{log_message}")
  score_cache['last_score'] = score
  score_cache['last_calc_period'] = current_period
  return score

# =============================================================================
# 网格系统与下单参数计算
# =============================================================================

stop_loss_limit = 0.02  # 停损价格阈值
take_profit_limit = 0.02  # 止盈价格阈值

def calculate_composite_score(current_price, last_order_price, last_s_order_price, stop_loss_limit, take_profit_limit):
    """
    根据最新价格及当前仓位、技术指标计算综合评分，并据此决定网格系统参数更新
    """
    global temp_ssbb, quantity_grid, quantity_grid_u, min_quantity, quantity_grid_rate
    global long_position, short_position
    data = kline_data_cache[interval_5m]
    if not data:
        logger.error("无K线数据")
        return temp_ssbb, 0
    if current_price is not None:
        data[-1][4] = current_price

    rsi = c_rsi(interval_5m, 14)
    mfi = c_mfi(interval_5m, 14)
    ema5 = c_ema(interval_5m, 5)
    macd, signal = c_macd(interval_ssbb_3m, fastperiod, slowperiod, signalperiod)
    percent_k, percent_d = c_so(interval_5m, 14, 3)
    v_ssbb, v_sbsb = c_ssbb(interval_ssbb_3m)
    ssbb, sbsb = int(v_ssbb.iloc[-1]), int(v_sbsb.iloc[-1])
    logger.info("******** 网格系统 ********")

    conditions_enabled = {
        'ssbb_logic': ssbb_logic_enabled,
        'price_change': price_change_enabled,
        'cost_price_logic': cost_price_logic_enabled,
        'technical_indicators': technical_indicators_enabled,
        'macd_signal': macd_signal_enabled
    }
    score = calculate_score(conditions_enabled)

    def calculate_liquidation_price(initial_margin_l, start_price_l, amplitude_percent_l, add_amount_l, leverage_l, trade_direction_l='BUY'):
        global max_position_size_long, max_position_size_short
        current_price_l = start_price_l
        total_quantity_l = long_position - short_position if trade_direction_l == 'BUY' else short_position - long_position
        total_cost_l = total_quantity_l * (long_cost if trade_direction_l == 'BUY' else short_cost)
        max_iterations = 100
        iteration_count = 0
        while iteration_count < max_iterations:
            iteration_count += 1
            if add_amount_l == 0:
                logger.info(f"添加量{add_amount_l}必须不为0")
                break
            total_quantity_l += add_amount_l
            if total_quantity_l == 0:
                total_quantity_l += add_amount_l
            total_cost_l += add_amount_l * current_price_l
            average_cost_l = total_cost_l / total_quantity_l if total_quantity_l > 0 else 0
            if trade_direction_l == 'BUY':
                loss_l = (average_cost_l - current_price_l) * total_quantity_l
            elif trade_direction_l == 'SELL':
                loss_l = (current_price_l - average_cost_l) * total_quantity_l
            if loss_l > initial_margin_l:
                if trade_direction_l == 'BUY' and max_position_size_long != round((total_quantity_l * LongRisk), dpq):
                    logger.info(f"原多单风控:{max_position_size_long}")
                    max_position_size_long = round((total_quantity_l * LongRisk), dpq)
                    status_manager.update_status('max_position_size_long', max_position_size_long)
                    logger.info(f"多单风控仓位{max_position_size_long}")
                elif trade_direction_l == 'SELL' and max_position_size_short != round((total_quantity_l * ShortRisk), dpq):
                    logger.info(f"原空单风控:{max_position_size_short}")
                    max_position_size_short = round((total_quantity_l * ShortRisk), dpq)
                    status_manager.update_status('max_position_size_short', max_position_size_short)
                    logger.info(f"空单风控仓位{max_position_size_short}")
                logger.info(f"{loss_l:.{dpq}f} > {initial_margin_l:.{dpq}f}")
                logger.info(f"头寸:{total_quantity_l:.{dpq}f}, 风控:{max_position_size_long if trade_direction_l == 'BUY' else max_position_size_short}, 持仓:{long_position if trade_direction_l == 'BUY' else short_position}")
                return round(current_price_l, dpp)
            if trade_direction_l == 'BUY':
                current_price_l -= (current_price_l * amplitude_percent_l) * leverage_l
            elif trade_direction_l == 'SELL':
                current_price_l += (current_price_l * amplitude_percent_l) * leverage_l
        return round(current_price_l, dpp)

    score_threshold = 25
    if abs(score) < score_threshold:
        sbsb = 0
        logger.info(f"\nRSI {rsi.iloc[-1]:.2f} MFI {mfi.iloc[-1]:.2f} MACD {macd.iloc[-1]:.2f} \n%K {percent_k.iloc[-1]:.2f} %D {percent_d.iloc[-1]:.2f}\n总分 {score} 未达到阈值 {score_threshold}，\nsbsb: {sbsb}, ssbb: {ssbb}")
    else:
        def is_price_change_significant(current_price, ref_price):
            global average_short_cost, average_long_cost
            price_change_ratio = abs(current_price - ref_price) / max(current_price, ref_price)
            significant_change = False
            trade_method = ""
            current_order_direction = 'BUY' if score >= score_threshold else 'SELL'
            liquidation_price = calculate_liquidation_price(initial_margin, current_price, FP, quantity_grid, 1, current_order_direction)
            if abs(price_change_ratio) > FP:
                if last_order_direction == current_order_direction:
                    if current_order_direction == 'BUY':
                        trade_method = "高价追买" if current_price > ref_price else "低价追买"
                        significant_change = False if current_price > ref_price else True
                    elif current_order_direction == 'SELL':
                        trade_method = "低价追卖" if current_price < ref_price else "高价追卖"
                        significant_change = False if current_price < ref_price else True
                else:
                    if current_order_direction == 'BUY':
                        trade_method = "亏损买平" if current_price > ref_price else "盈利买平"
                        significant_change = False if current_price > ref_price else True
                    elif current_order_direction == 'SELL':
                        trade_method = "亏损卖平" if current_price < ref_price else "盈利卖平"
                        significant_change = False if current_price < ref_price else True
            else:
                trade_method = "频繁提交"
            logger.info(f"相对于 {ref_price}，以 {current_price} 执行 {trade_method}" if significant_change else f"相对于 {ref_price}，禁止 {current_price} 执行 {trade_method}")
            return significant_change, price_change_ratio

        def make_decision(action, significant_change, sbsb_value, ssbb_value):
            if significant_change:
                return action, ssbb_value, sbsb_value
            else:
                action += " 价格变化不足"
                return action, ssbb_value, 0

        sbsb = 0
        action = "无操作"
        ref_price = last_order_price if last_order_direction == 'BUY' else last_s_order_price
        significant_change, price_change_ratio = is_price_change_significant(current_price, ref_price)
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
        logger.info(f"价格变化比例：{price_change_ratio:.2%}，触发比例 {FP:.2%}")
        logger.info(f"sbsb: {sbsb}, ssbb: {ssbb}")
    if temp_ssbb != ssbb:
        temp_ssbb = ssbb
        status_manager.update_status('ssbb', ssbb)
        logger.info(f"ssbb 更新为 {ssbb}")

    global min_quantity
    if min_quantity_u >= 5:
        calculated_min_quantity = math.ceil(min_quantity_u / current_price)
        if min_quantity != calculated_min_quantity:
            min_quantity = calculated_min_quantity
            print(f"更新最小增仓量：{min_quantity}")
            status_manager.update_status('min_quantity', min_quantity)

    global quantity_grid, quantity_grid_u
    absolute_position_difference = abs(long_position - short_position)
    calculated_quantity_grid_based_on_rate = math.ceil(quantity_grid_rate * 0.01 * absolute_position_difference)
    calculated_quantity_grid_based_on_u = math.ceil(quantity_grid_u / current_price)
    calculated_quantity_grid_for_min = math.ceil(min_quantity_u / current_price)
    quantity_grid_u = max(quantity_grid_u, 5)
    new_quantity_grid = None
    if quantity_grid_rate > 0 and calculated_quantity_grid_based_on_rate > max(quantity_grid, min_quantity):
        new_quantity_grid = calculated_quantity_grid_based_on_rate
        update_message = "更新单位网格量1："
    elif quantity_grid_u > 5 and quantity_grid != calculated_quantity_grid_based_on_u:
        new_quantity_grid = calculated_quantity_grid_based_on_u
        update_message = "更新单位网格量2："
    elif quantity_grid < calculated_quantity_grid_for_min:
        new_quantity_grid = calculated_quantity_grid_for_min
        update_message = "更新单位网格量到最小值："
    if new_quantity_grid is not None and new_quantity_grid != quantity_grid:
        quantity_grid = new_quantity_grid
        print(update_message + f"{quantity_grid}")
        status_manager.update_status('quantity_grid', quantity_grid)
    return ssbb, sbsb

def calculate_next_order_parameters(price, leverage, order_position):
    """
    根据当前价格、杠杆及订单意图计算下一次下单价格及交易量参数。
    """
    global last_order_direction
    try:
        next_price = round(price, dpp)
        reference_price = last_order_price if last_order_direction == 'BUY' else last_s_order_price
        if reference_price and reference_price != 0 and next_price != reference_price:
            grid_ratio = max(current_price, reference_price) / min(current_price, reference_price)
            grid_count = int(math.log(grid_ratio) / math.log(1 + FP))
            logger.info(f"1 优化前网格数量: {grid_count}")
            grid_count = (1 / (1 + math.exp(-grid_count / 6)) - 0.5) * 2 * 15
            logger.info(f"1 S型函数优化后的网格数量: {grid_count:.1f}\n")
        elif reference_price == 0:
            grid_count = 1
            logger.info(f"首次 {last_order_direction} 下单: {grid_count}\n")
        else:
            grid_count = 0
            logger.info(f"差异小于阈值 FP: {grid_count}\n")
        if order_position == 'lb':
            origQty = adjust_quantity(quantity_grid * grid_count * (1 + FP * grid_adjustment_factor))
        elif order_position == 'ss':
            origQty = adjust_quantity(quantity_grid * grid_count * (1 - FP * grid_adjustment_factor))
        logger.info(f"2 优化前交易量: {origQty}")
        origQty = adjust_quantity(max(1 / (1 + math.exp(4 - max(continuous_add_count_lb, continuous_add_count_ss))) * 2 * origQty, min_quantity))
        logger.info(f"2 S型函数优化交易量: {origQty} (lb计数: {continuous_add_count_lb} / ss计数: {continuous_add_count_ss})")
        return next_price, origQty
    except Exception as e:
        logger.error(f"执行 calculate_next_order_parameters 出错: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        return None, None

def adjust_quantity(ad_quantity):
    """调整下单量以满足最小下单量和步进要求。"""
    if ad_quantity < min_quantity:
        logger.error(f"下单量小于最小下单量: {ad_quantity}")
        return 0
    ad_quantity = math.ceil(ad_quantity / step_size) * step_size
    logger.info(f"调整后的下单量: {ad_quantity}")
    return ad_quantity

def update_order_status(response):
    """
    根据服务器返回的订单响应更新订单状态及相关全局变量。

    优化思路：
      - 不依赖外部传入的 position 参数，而是直接根据 response 中的 side 和 positionSide 判断订单标签
      - 更新持仓成本（调用 update_position_cost ）
      - 根据订单方向更新连续追加计数及平均持仓成本、数量（通过内部函数 update_position 完成）
      - 更新 last_order_price/last_s_order_price 及订单ID
      - 最后通过 status_manager 将所有更新的状态保存

    参数:
      response: 服务器返回的订单响应字典，必须包含 'price', 'origQty', 'type', 'side', 'positionSide', 'orderId' 等字段
    """
    global stime, ltime, long_position, short_position, last_order_price, last_s_order_price, FP, add_rate, quantity, last_order_direction
    global average_long_cost, average_short_cost, average_long_position, average_short_position, quantity_grid_rate
    try:
        # 检查必须字段
        required_keys = ['price', 'origQty', 'type', 'side', 'positionSide', 'orderId']
        if not all(key in response for key in required_keys):
            logger.error("响应中缺失必要字段，无法更新订单状态")
            return

        # 获取更新价格（对于 TRAILING_STOP_MARKET 尝试从 activatePrice 获取）
        if response['type'] != 'TRAILING_STOP_MARKET':
            update_price = float(response['price'])
        else:
            update_price = float(response.get('activatePrice', response['price']))
        quantity_response = float(response['origQty'])

        logger.info("#######################")
        logger.info("======= cpo 成功 ==========")
        logger.info("#######################")
        logger.info("=======================")
        logger.info(f"cpo 成功 {response['side']}：{update_price}")

        # 更新持仓成本（函数内部已处理多头和空头的逻辑）
        update_position_cost(update_price, quantity_response, response['positionSide'], response['side'])

        # 内部辅助函数：根据订单 side 更新平均成本和持仓数量
        def update_position(side, update_price, quantity_response, transaction_fee_rate,
                            avg_long_cost, avg_long_pos, avg_short_cost, avg_short_pos):
            global continuous_add_count_lb, continuous_add_count_ss
            side = side.upper()
            if side == 'BUY':
                continuous_add_count_lb += 1.5
                continuous_add_count_ss = 0.5
                # 如果存在空头持仓，则先减仓
                if avg_short_pos > 0:
                    reduced_quantity = min(avg_short_pos, quantity_response)
                    avg_short_pos -= reduced_quantity
                    quantity_response -= reduced_quantity
                    if avg_short_pos == 0:
                        avg_short_cost = 0
                    if quantity_response <= 0:
                        return avg_long_cost, avg_long_pos, avg_short_cost, avg_short_pos
                total_cost = avg_long_cost * avg_long_pos + update_price * quantity_response * (1 + transaction_fee_rate)
                avg_long_pos += quantity_response
                avg_long_cost = total_cost / avg_long_pos if avg_long_pos > 0 else 0
            elif side == 'SELL':
                continuous_add_count_lb = 1.5
                continuous_add_count_ss += 0.5
                # 如果存在多头持仓，则先减仓
                if avg_long_pos > 0:
                    reduced_quantity = min(avg_long_pos, quantity_response)
                    avg_long_pos -= reduced_quantity
                    quantity_response -= reduced_quantity
                    if avg_long_pos == 0:
                        avg_long_cost = 0
                    if quantity_response <= 0:
                        return avg_long_cost, avg_long_pos, avg_short_cost, avg_short_pos
                total_cost = avg_short_cost * avg_short_pos + update_price * quantity_response * (1 - transaction_fee_rate)
                avg_short_pos += quantity_response
                avg_short_cost = total_cost / avg_short_pos if avg_short_pos > 0 else 0
            status_manager.update_status('continuous_add_count_lb', continuous_add_count_lb)
            status_manager.update_status('continuous_add_count_ss', continuous_add_count_ss)
            return avg_long_cost, avg_long_pos, avg_short_cost, avg_short_pos

        # 更新平均成本和仓位数量（根据 response['side'] 及其他参数）
        average_long_cost, average_long_position, average_short_cost, average_short_position = update_position(
            response['side'], update_price, quantity_response, transaction_fee_rate,
            average_long_cost, average_long_position, average_short_cost, average_short_position
        )

        logger.info(f"更新后的持仓 - 多头成本: {average_long_cost}, 多头持仓: {average_long_position}, "
                    f"空头成本: {average_short_cost}, 空头持仓: {average_short_position}")
        status_manager.update_status('average_long_cost', average_long_cost)
        status_manager.update_status('average_short_cost', average_short_cost)
        status_manager.update_status('average_long_position', average_long_position)
        status_manager.update_status('average_short_position', average_short_position)

        # 根据 response 中的 side 与 positionSide 自动确定订单标签
        side_resp = response['side'].upper()
        pos_side_resp = response['positionSide'].upper()
        order_tag = None
        if pos_side_resp == "LONG" and side_resp == "BUY":
            order_tag = "lb"
        elif pos_side_resp == "LONG" and side_resp == "SELL":
            order_tag = "ls"
        elif pos_side_resp == "SHORT" and side_resp == "SELL":
            order_tag = "ss"
        elif pos_side_resp == "SHORT" and side_resp == "BUY":
            order_tag = "sb"
        else:
            logger.warning(f"无法根据 side:{side_resp} 与 positionSide:{pos_side_resp} 确定订单标签")

        logger.info(f"将更新价格设为 {order_tag}: {update_price}")
        if order_tag in ('lb', 'sb'):
            last_order_price = update_price
            last_order_orderId = response['orderId']
            status_manager.update_status('last_order_orderId', last_order_orderId)
        elif order_tag in ('ls', 'ss'):
            last_s_order_price = update_price
            last_s_order_orderId = response['orderId']
            status_manager.update_status('last_s_order_orderId', last_s_order_orderId)
        else:
            logger.warning("订单标签为空，无法更新 last_order_price/last_s_order_price")

        status_manager.update_status('last_order_price', last_order_price)
        status_manager.update_status('last_s_order_price', last_s_order_price)

        # 更新单位网格量（quantity）—如果不等于 math.ceil(quantity_u/current_price) 则更新
        if quantity != math.ceil(quantity_u / current_price):
            if quantity_u >= 5:
                quantity = math.ceil(quantity_u / current_price)
                print(f"更新单格量：{quantity}")
                status_manager.update_status('quantity', quantity)

        # 根据 martingale 策略调整下单量
        quantity = float(quantity - min_quantity) * float(martingale) + min_quantity
        status_manager.update_status('quantity', quantity)

        # 根据杠杆调整 FP, add_rate, quantity_grid_rate
        FP = float(FP) * float(leverage)
        add_rate = float(add_rate) * float(leverage)
        quantity_grid_rate = float(quantity_grid_rate) * float(leverage)
        status_manager.update_status('FP', FP)
        status_manager.update_status('add_rate', add_rate)
        status_manager.update_status('quantity_grid_rate', quantity_grid_rate)

        last_order_direction = response['side']
        status_manager.update_status('last_order_direction', last_order_direction)

        time_str = current_time.strftime("%H:%M:%S")
        logger.info("\n" + "=" * 50 + f"\n== 订单时间: {time_str} " + " " * (30 - len(time_str)) + "==\n" +
                    f"== 当前价格: {current_price:.{dpp}f} " + " " * (37 - len(str(current_price))) + "==\n" +
                    f"== 数量: {response['origQty']} | 方向: {order_tag if order_tag else '未知'} | 价格: {update_price:.{dpp}f} " +
                    " " * (10 - len(str(update_price)) - len(str(response['origQty'])) - len(order_tag if order_tag else "")) + "==\n" +
                    f"== 订单ID: {response['orderId']} " + " " * (40 - len(str(response['orderId']))) + "==\n" +
                    "=" * 50 + "\n")

        # 保存订单记录
        order_record = {
            'order_time': time_str,
            'order_id': response['orderId'],
            'type': response['type'],
            'positionSide': response['positionSide'],
            'side': response['side'],
            'Price': response['price'],
            'quantity': response['origQty'],
            'status': response['status']  # 初始状态设为未成交
        }
        update_order_records(order_record)
        logger.info(f"暂停3, {order_interval}")
        time.sleep(order_interval)  # 等待指定的下单间隔时间

    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}")
        logger.debug(traceback.format_exc())

def query_order(order_id):
    try:
        response = client.query_order(symbol=symbol, orderId=order_id, recvWindow=5000)
        logger.info(response)
    except ClientError as error:
        logger.info(f"w2: {error}")
        logger.error(f"查询订单时出错。状态: {error.status_code}, 错误码: {error.error_code}, 错误消息: {error.error_message}")
    except Exception as e:
        logger.error(f"查询订单时发生未预期的错误: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


def handle_client_error(error, current_price, order_params):
    error_messages = {-1021: "时间同步问题", 400: "请求过多或订单超时", -1003: "请求过多或订单超时"}
    logger.info(f"w3: {error}")
    logger.error(f"下单时出错。状态: {error.status_code}, 错误码: {error.error_code}, 错误消息: {error.error_message}, 最新价格：{current_price}, 下单参数: {order_params}")
    error_message = error_messages.get(error.error_code, "未知错误")
    logger.error(f"发生错误: {error_message}")
    if error.error_code in [-1021, 400, -1003]:
        logger.info(f"暂停6, {ltime}")
        time.sleep(ltime)
    else:
        logger.info(f"暂停7, {12}")
        time.sleep(12)


def dynamic_tracking(symbol, trade_direction, callback_rate, optimal_price, trade_quantity, start_order_price=None, start_price_reached=False, trade_executed=False):
    global current_price, add_position_1
    try:
        logger.info(f"****** 动态追踪 {trade_direction} ******")
        logger.info(f"触发 {start_order_price}: {start_price_reached} 成交 {trade_executed} 回调率 {round(callback_rate, dpp)} 最佳 {optimal_price} 量 {trade_quantity}")
        # 参数校验
        if trade_direction not in ['lb', 'ss']:
            raise ValueError(f"交易方向 {trade_direction} 必须是 'lb' 或 'ss'")
        if not all(isinstance(x, (int, float)) and x > 0 for x in [callback_rate, optimal_price, trade_quantity]):
            raise ValueError(f"回调率 {round(callback_rate, dpp)}、最佳价格 {optimal_price} 和交易数量 {trade_quantity} 必须是正数")
        if start_order_price is not None and not isinstance(start_order_price, (int, float)):
            raise ValueError(f"启动价格 {start_order_price} 必须是数字")
        if trade_executed == True:
            raise ValueError("交易已执行，请勿重复执行")
        # 获取当前市场价格
        current_price, last_price_update_time = get_current_price(symbol)
        # 检查当前价格是否达到或超过启动价格
        if start_order_price is None and start_price_reached == False:
            start_order_price = round(optimal_price * (1 - add_rate if trade_direction == 'lb' else 1 + add_rate), dpp)
            logger.info(f"启动价格为 None 且未触发，则采用最佳价格调整后的 {round(add_rate, dpp)}：{start_order_price}")
        if (trade_direction == 'lb' and start_order_price is not None and current_price <= start_order_price) or \
           (trade_direction == 'ss' and start_order_price is not None and current_price >= start_order_price):
            start_price_reached = True
            logger.info(f"触发启动价格：{start_order_price}")
        if start_price_reached:
            # 计算价格变化比例
            price_change = abs(current_price - optimal_price) / optimal_price
            # 检查是否达到回调率阈值
            if price_change >= callback_rate:
                if trade_direction == 'lb' and current_price > optimal_price:
                    logger.info("上涨达到回调率，执行买入操作")
                    if place_limit_order(symbol, trade_direction, 'QUEUE_5', trade_quantity, callback):
                        trade_executed = True
                        start_price_reached = False
                        trade_quantity = 0
                elif trade_direction == 'ss' and current_price < optimal_price:
                    logger.info("下跌达到回调率，执行卖出操作")
                    if place_limit_order(symbol, trade_direction, 'QUEUE_5', trade_quantity, callback):
                        trade_executed = True
                        start_price_reached = False
                        trade_quantity = 0
                else:
                    logger.error("无效的交易方向")
            status_manager.update_status('add_position_1', add_position_1)
            # 如果当前价格更有利，则更新最佳价格
            if (trade_direction == 'lb' and current_price < optimal_price) or \
               (trade_direction == 'ss' and current_price > optimal_price):
                optimal_price = current_price
                logger.info(f"更新最佳价格：{optimal_price}")
        return start_price_reached, trade_executed, optimal_price, trade_quantity
    except ValueError as e:
        logger.error(f"参数非法: {e}")
        return start_price_reached, trade_executed, optimal_price, trade_quantity
    except Exception as e:
        logger.error(f"执行动态追踪时发生错误: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        return False, True, optimal_price, trade_quantity


def breakeven_stop_profit(symbol, trade_direction, breakeven_price, trade_quantity, start_order_price=None, start_price_reached=False, trade_executed=False):
    global current_price
    try:
        logger.info(f"执行保本止盈 {trade_direction}")
        logger.info(f"触发状态 {start_price_reached}:{start_order_price} 成交 {trade_executed} 保本价 {breakeven_price} 交易量 {trade_quantity}")
        if trade_direction not in ['lb', 'ss']:
            raise ValueError(f"交易方向 {trade_direction} 必须是 'lb' 或 'ss'")
        if breakeven_price <= 0 or trade_quantity <= 0:
            raise ValueError(f"保本价格 {breakeven_price} 和交易数量 {trade_quantity} 必须大于 0")
        if start_order_price is not None and start_order_price <= 0:
            raise ValueError(f"启动价格 {start_order_price} 必须大于 0")
        if trade_quantity < min_quantity:
            raise ValueError(f"交易数量 {trade_quantity} 必须大于等于最小下单量 (min_quantity)")
        decimal_places = 3
        trade_quantity_float = float(trade_quantity)
        step_size_float = float(step_size)
        adjusted_quantity = round(trade_quantity_float / step_size_float, decimal_places)
        if not adjusted_quantity.is_integer():
            raise ValueError(f"交易数量 {trade_quantity} 必须是最小调整步长 {step_size} 的整数倍")
        if trade_executed == True:
            raise ValueError("交易已执行，请勿重复执行")
        current_price, last_price_update_time = get_current_price(symbol)
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
                logger.info(f"达到保本价，执行 {trade_direction} 操作。当前价格: {current_price}")
                if place_limit_order(symbol, trade_direction, 'QUEUE_5', trade_quantity, callback):
                    trade_executed = True
                    start_price_reached = False
                    trade_quantity = 0
            else:
                logger.info("当前价格未达到保本止盈点")
        return start_price_reached, trade_executed, trade_quantity
    except ValueError as e:
        logger.error(f"参数错误: {e}")
        return start_price_reached, trade_executed, trade_quantity
    except Exception as e:
        logger.error(f"执行保本止盈时发生错误: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        return False, True, trade_quantity


def trading_strategy():
    # 注：启动成本 starta_cost 此处改为从 current_status 获取
    global starta_price, starta_position, starta_cost, trade_executed_1, start_price_reached_1, optimal_price_1, trade_executed_2, start_price_reached_2, breakeven_price_2, trade_executed_3, start_price_reached_3, optimal_price_3, trade_executed_4, start_price_reached_4, breakeven_price_4, add_position_1, add_position
    logger.info(f"******** {starta_direction} 对冲中 ********")
    try:
        if trading_strategy_enabled == 0:
            logger.info("交易策略未启用")
            return
        if add_position_rate > 0 and add_position != max(math.ceil(add_position_rate * 0.01 * ((long_position - short_position) if starta_direction == "lb" else (short_position - long_position))), math.ceil(max(5.1, add_position_u) / current_price)):
            add_position = max(math.ceil(add_position_rate * 0.01 * ((long_position - short_position) if starta_direction == "lb" else (short_position - long_position))), math.ceil(max(5.1, add_position_u) / current_price))
            logger.info(f"当前 {round(add_position_rate * 0.01, 3)} 更新单位对冲量：{add_position}")
            status_manager.update_status('add_position', add_position)
        if add_position_rate <= 0 and add_position != math.ceil(add_position_u / current_price):
            if add_position_u >= 5:
                add_position = math.ceil(add_position_u / current_price)
                logger.info(f"更新单位对冲量：{add_position}")
                status_manager.update_status('add_position', add_position)
        logger.info(f"启动仓位：{starta_position}")
        if starta_position == 0:
            logger.info("未开仓，初始化对冲")
            trade_executed_1, start_price_reached_1, optimal_price_1, trade_executed_2, start_price_reached_2, breakeven_price_2, trade_executed_3, start_price_reached_3, optimal_price_3, trade_executed_4, start_price_reached_4, breakeven_price_4 = True, False, 0, True, False, 0, True, False, 0, True, False, 0
            starta_price = current_price
            trigger_price = starta_price
            starta_cost = starta_price if long_position == short_position == 0 else (long_cost if long_position >= short_position else short_cost)
            add_position_1 = 0
            logger.info(f"触发价格1：{trigger_price}")
            starta_position = add_position if add_position > 0 else 1
        trigger_price = round((min(starta_price if starta_cost == 0 or starta_cost is None else starta_cost, starta_price) if starta_direction == 'lb' else max(starta_cost, starta_price)) * (1 - add_rate if starta_direction == 'lb' else 1 + add_rate), dpp)
        logger.info(f"触发价格2：{trigger_price}")
        profit_price = round(starta_cost * (1 - add_rate if starta_direction == 'ss' else 1 + add_rate), dpp)
        if starta_price == 0:
            starta_price, last_price_update_time = get_current_price(symbol)
            logger.info(f"启动价格调整为当前价格：{starta_price}")
        if starta_cost == 0 or (starta_cost < starta_price and starta_direction == 'lb') or (starta_cost > starta_price and starta_direction == 'ss'):
            starta_cost = starta_price
            logger.info(f"启动成本调整为启动价格：{starta_cost}")
        logger.info(f"-开始 {starta_direction} 对冲策略，当前价格：{current_price}, 启动价格：{starta_price}")
        logger.info(f"-启动仓位：{starta_position}, 启动成本：{starta_cost}")
        logger.info(f"-追加幅度：{round(add_rate, dpp)}, 单位追加量：{add_position}")
        logger.info(f"-追加价格：{trigger_price}, 待交易量：{add_position_1}, 止盈价格：{profit_price}")
        conditions_enabled = {
            'ssbb_logic': ssbb_logic_enabled,
            'price_change': price_change_enabled,
            'cost_price_logic': cost_price_logic_enabled,
            'technical_indicators': technical_indicators_enabled,
            'macd_signal': macd_signal_enabled
        }
        score = calculate_score(conditions_enabled)
        add_direction = 'lb' if starta_direction == 'ss' else 'ss'
        if not trade_executed_1 and add_position_1 != 0:
            logger.info(f"动态追加1 {trade_executed_1}，触发状态 {start_price_reached_1}：最佳价格 {optimal_price_1}")
            add_position_temp = add_position_1
            start_price_reached_1, trade_executed_1, optimal_price_1, add_position_1 = dynamic_tracking(
                symbol, starta_direction, add_rate, optimal_price_1, add_position_1,
                start_price_reached=start_price_reached_1, trade_executed=trade_executed_1)
            if add_position_temp != add_position_1:
                starta_position += add_position_temp  # 更新持仓量
                trade_executed_2 = True
                logger.info(f"动态追加1 {trade_executed_1}，交易完成，更新持仓量：{starta_position}；保价强追2更新 {trade_executed_2}")
            logger.info(f"动态追加1 {trade_executed_1}，触发状态 {start_price_reached_1}：最佳价格 {optimal_price_1}")
        if not trade_executed_2 and breakeven_price_2 != 0:
            logger.info(f"保价强追2 {trade_executed_2}，触发状态 {start_price_reached_2}：保本价格 {breakeven_price_2}")
            add_position_temp = add_position_1
            start_price_reached_2, trade_executed_2, add_position_1 = breakeven_stop_profit(
                symbol, starta_direction, breakeven_price_2, add_position_1,
                start_price_reached=start_price_reached_2, trade_executed=trade_executed_2)
            if add_position_temp != add_position_1:
                starta_position += add_position_temp  # 更新持仓量
                trade_executed_1 = True
                logger.info(f"保价强追2 {trade_executed_2}，交易完成，更新持仓量：{starta_position}；动态追加1更新 {trade_executed_1}")
            else:
                logger.info(f"保价强追2 {trade_executed_2}，触发状态 {start_price_reached_2}：保本价格 {breakeven_price_2}")
        if not trade_executed_3 and starta_position != add_position:
            logger.info(f"动态追平3 {trade_executed_3}，触发状态 {start_price_reached_3}：最佳价格 {optimal_price_3}")
            profit_position_temp = profit_position
            start_price_reached_3, trade_executed_3, optimal_price_3, profit_position = dynamic_tracking(
                symbol, add_direction, add_rate, optimal_price_3, profit_position,
                start_price_reached=start_price_reached_3, trade_executed=trade_executed_3)
            if profit_position_temp != profit_position:
                starta_position = add_position
                optimal_price_3 = 0
                trade_executed_4 = True
                starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), dpp)
                logger.info(f"动态追平3 {trade_executed_3}，交易完成，更新持仓量 {starta_position}，启动价格 {starta_price}；保价强平4更新 {trade_executed_4}")
            else:
                logger.info(f"动态追平3 {trade_executed_3}，触发状态 {start_price_reached_3}：最佳价格 {optimal_price_3}")
        if not trade_executed_4 and starta_position != add_position:
            logger.info(f"保价强平4 {trade_executed_4}，触发状态 {start_price_reached_4}：保本价格 {breakeven_price_4}")
            profit_position_temp = profit_position
            start_price_reached_4, trade_executed_4, profit_position = breakeven_stop_profit(
                symbol, add_direction, breakeven_price_4, profit_position,
                start_price_reached=start_price_reached_4, trade_executed=trade_executed_4)
            if profit_position_temp != profit_position:
                starta_position = add_position
                optimal_price_3 = 0
                trade_executed_3 = True
                starta_price = round(starta_cost * (1 - add_rate / 2 if starta_direction == 'ss' else 1 + add_rate), dpp)
                logger.info(f"动态追平3 {trade_executed_3}，交易完成，更新持仓量 {starta_position}，启动价格 {starta_price}；保价强平4更新 {trade_executed_4}")
            else:
                logger.info(f"保价强平4 {trade_executed_4}，触发状态 {start_price_reached_4}：保本价格 {breakeven_price_4}")
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
        logger.info(f"-追加价格：{trigger_price}, 待交易量：{add_position_1}, 止盈价格：{profit_price}")
    except Exception as e:
        logger.error(f"执行 trading_strategy 时发生错误: {e}")
        traceback.print_exc()
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")


# =============================================================================
# 配置加载、定时任务与主循环
# =============================================================================

def switch_config():
    global current_config
    return 'b' if current_config == 'a' else 'a'

def get_config_file():
    return f'config_{current_config}.txt'

def get_alternate_config_file():
    alternate = switch_config()
    status_manager.update_status('current_config', alternate)
    return f'config_{alternate}.txt'

# 下单交易
def place_limit_order(symbol, position, price, quantitya, callback=0.4):

  global stime, ltime, long_position, short_position, last_order_price, last_s_order_price, client, logger, FP, last_order_direction
  logger.info(f"#######################")
  logger.info(f"=======调用下单========")
  logger.info(f"###{price}#{position}#{quantitya}###")
  logger.info(f"=======================")
  if is_paused:
    logger.info("脚本暂停执行")
    return
  if position not in ['lb', 'ss']:
    logging.error(f"无效的订单意图：{position}")
    return
  quantitya = quantitya if quantitya is not None else 0.0
  adjusted_quantity = quantitya
  if position == 'lb':
      max_allowed = max_position_size_long - (long_position - short_position)
      if quantitya > max_allowed:
          logging.warning(f"多单风控调整：原始数量{quantitya}，调整后{max_allowed}")
          adjusted_quantity = max_allowed / 6
  elif position == 'ss':
      max_allowed = max_position_size_short - (short_position - long_position)
      if quantitya > max_allowed:
          logging.warning(f"空单风控调整：原始数量{quantitya}，调整后{max_allowed}")
          adjusted_quantity = max_allowed / 6
  # 确保调整后的数量不为负
  quantity = max(min_quantity, adjusted_quantity)
  if quantity == min_quantity:
      logging.error(f"{position}风控触发，调整下单量至min_quantity。多仓：{long_position}，空仓：{short_position}")
      quantitya = quantity
    #  return  调整下单量至min_quantity
  else:
      logging.error(f"{position}风控未触发，{long_position - short_position}多仓未触及{max_position_size_long}，{short_position - long_position}空单未触及{max_position_size_long}")
  if callback < Slippage * 100:
    callback = min(Slippage * 100, 0.4)

  origQty = adjust_quantity(quantitya)
  logger.info(f"下单价格: {price}")
  price_is_special = price in [
    "OPPONENT", "OPPONENT_5", "OPPONENT_10", "OPPONENT_20", "QUEUE", "QUEUE_5",
    "QUEUE_10", "QUEUE_20"
  ]
  if not price_is_special:
    try:
      if float(price) > 0:
        price = round(
          price * (1 - Slippage if position == 'lb' else 1 + Slippage), dpp)
        logger.info(f"slippage优化下单价格: {price}")
      elif float(price) > 0:
        logging.error(f"无效的price：{price}")
        return
    except ValueError:
      logger.error(f"无效的价格值：{price}")
      return
  if origQty <= 0:
    logger.error("下单量必须大于0")
    return

  # 根据持仓量调整下单逻辑
  def determine_order_side():
    LONG_BUY = 'lb'
    SHORT_SELL = 'ss'
    NONE = 'none'

    if position not in [LONG_BUY, SHORT_SELL]:
      logging.error(f"无效的订单意图：{position}")
      return NONE

    if force_reduce or (position == LONG_BUY and short_position >= origQty) or (position == SHORT_SELL and long_position >= origQty):
      logger.info(f"强制减仓{force_reduce},优先减仓{position}:{short_position if position == LONG_BUY else long_position}")
      return ('BUY', 'SHORT') if position == LONG_BUY else ('SELL', 'LONG')
    elif (position == LONG_BUY and long_position - short_position + origQty <= max_position_size_long) or (position == SHORT_SELL and short_position - long_position + origQty <= max_position_size_short):
      return ('BUY', 'LONG') if position == LONG_BUY else ('SELL', 'SHORT')
    return NONE

  if determine_order_side() == 'none':
    logger.error("下单逻辑出错")
    return
  else:
    order_side, position_side = determine_order_side()

  if order_side == 'BUY' and position_side == 'SHORT':
    if short_position < origQty:
        logger.info(f"强制下单，{force_reduce}风控{max_position_size_long}")
        return
  elif order_side == 'SELL' and position_side == 'LONG':
    if long_position < origQty:
        logger.info(f"强制下单，{force_reduce}风控{max_position_size_short}")
        return

  def create_trailing_stop_order_params(order_side, position_side, price,
                                        callback, origQty):
    logger.info(f"动态追踪{order_side}优化激活价格: {price}*0.02 * callback")
    # 'activationPrice': round(price * (1 - 0.02 * callback) if order_side == 'BUY' else price * (1 + 0.02 * callback), dpp),
    return {
      'symbol': symbol,
      'side': order_side,
      'positionSide': position_side,
      'type': "TRAILING_STOP_MARKET",
      'activationPrice': round(price, dpp),
      'callbackRate': callback,
      'quantity': math.floor(origQty * 1000) / 1000,
      'timeInForce': time_in_force,
    }

  def create_standard_order_params(order_side, position_side, price, origQty):
    logger.info(f"限价单{order_side}: {price}")
    return {
      'symbol': symbol,
      'side': order_side,
      'positionSide': position_side,
      'type': order_type,
      'timeInForce': time_in_force,
      'quantity': math.floor(origQty * 1000) / 1000,
      'price': round(price, dpp),
      'priceProtect': False,
      'closePosition': False
    }

  def create_take_profit_order_params(order_side, position_side, price,
                                      origQty):
    logger.info(f"止盈单{order_side}优化止盈激活价格: {price}*0.005 * callback")
    logger.info(f"止盈单{order_side}优化止盈价格: {price}*0.02 * callback")
    return {
      'symbol':
      symbol,
      'side':
      order_side,
      'positionSide':
      position_side,
      'type':
      "TAKE_PROFIT",  # STOP, TAKE_PROFIT
      'stopPrice':
      round(price, dpp),
      'price':
      round((price - (min_price_step * 2)) if order_side == 'BUY' else
            (price + (min_price_step * 2)), dpp),
      'quantity':
      math.floor(origQty * 1000) / 1000,
      'timeInForce':
      time_in_force,
    }

  def create_match_order_params(order_side, position_side, price, origQty):
    logger.info(f"限价单{order_side}: {price}")
    return {
      'symbol': symbol,
      'side': order_side,
      'positionSide': position_side,
      'type': order_type,
      'timeInForce': time_in_force,
      'quantity': math.floor(origQty * 1000) / 1000,
      'priceMatch': price,
      #"OPPONENT"、"OPPONENT_5"、"OPPONENT_10"、"OPPONENT_20"、"QUEUE"、"QUEUE_5"、"QUEUE_10"、"QUEUE_20"
      'priceProtect': False,
      'closePosition': False
    }

  try:
    if order_side == 'none':
      logger.error("order_side为none，停止下单")
      return
    if price_is_special:
      logger.info(f"匹配订单参数")
      order_params = create_match_order_params(order_side, position_side,
                                               price, origQty)
    else:
      if (order_side == 'BUY'
          and short_position > origQty) or (order_side == 'SELL'
                                            and long_position > origQty):
        logger.info(f"限价止盈平仓")
        order_params = create_take_profit_order_params(order_side,
                                                       position_side, price,
                                                       origQty)
      elif (order_side == 'BUY'
            and position_side == 'SHORT') or (order_side == 'SELL'
                                              and position_side == 'LONG'):
        logger.info(f"动态追踪平仓")
        order_params = create_trailing_stop_order_params(
          order_side, position_side, price, callback, origQty)
      else:
        logger.info(f"限价开仓")
        order_params = create_standard_order_params(order_side, position_side,
                                                    price, origQty)
    current_time, time_str = get_current_time()  # 获取时间和时间字符串
    current_price, last_price_update_time = get_current_price(symbol, now=1)
    get_binance_server_time()
    response = client.new_order(**order_params)
    return response
  except ClientError as error:
    logger.info(f"w1: {error.args[1]}")
    if error.args[1] == -2022:
        logger.error("ReduceOnly Order is rejected. 尝试重新获取并更新当前持仓。")
        try:
            fetch_and_update_active_orders(symbol)
            logger.info("更新当前持仓。")
            # 根据更新后的持仓情况，决定是否重新尝试下单或执行其他逻辑
            # 例如，可以重新计算 `quantity` 并尝试再次下单
            # 以下是一个简单的重新尝试下单的示例（需根据实际情况调整）
          #  response = place_limit_order(symbol, position, price, quantitya, callback)
          #  return response
        except Exception as update_error:
            logger.error(f"更新持仓时发生错误: {update_error}")
            logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
            logger.info("暂停12秒后继续执行。")
            time.sleep(12)
    elif error.args[1] == -4045:
      # 当达到最大止损订单限制时，转为市价建仓方式下单
      order_params = create_standard_order_params(order_side, position_side,
                                                  price, origQty)
      try:
        update_order_status(order_params)
        logger.info(f"止损单限制，转为市价建仓方式下单")
        return order_params
      except Exception as e:
        logger.error(f"市价建仓下单时发生错误: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        time.sleep(monitoring_interval)  # 发生错误时等待一分钟
    else:
      logger.error(
        f"下单时出错。状态: {error.args[0]}, 错误码: {error.args[1]}, 错误消息: {error.args[2]}, 最新价格：{current_price}, 下单参数: {order_params}"
      )
      logger.error(f"API错误: {error}")
    logger.info(f"暂停4,12s")
    time.sleep(12)

  except Exception as e:
    logger.error(f"Unexpected error: {e}")
    logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
    logger.info(f"暂停5,12s")
    time.sleep(12)

def execute_config_file(config_file_path):
    """
    执行指定配置文件中的 Python 脚本。
    参数：
      config_file_path：配置文件路径（相对于 BASE_PATH）。
    """
    config_file = build_file_path(config_file_path)
    try:
        logger.info(f"加载配置 {current_config} ...")
        with open(config_file, 'r', encoding='utf-8') as file:
            script = file.read()
            exec(script, globals())
    except FileNotFoundError:
        logger.error(f"{config_file} 文件未找到。")
    except SyntaxError as e:
        logger.error(f"{config_file} 中存在语法错误: {e}")
    except Exception as e:
        logger.error(f"读取 {config_file} 时发生错误: {e}")
        traceback.print_exc()
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

async def fetch_and_update_config():
    """
    异步从GitHub获取配置文件最新提交时间，如检测更新则下载最新配置并执行。
    """
    global current_config, last_config_update_time
    try:
        config = load_config()
        token = config['github_api_key']
        repo = 'kongji1/go'
        file_path = 'go.txt'
        api_url = f'https://api.github.com/repos/{repo}/commits?path={file_path}'
        headers = {'Authorization': f'token {token}'}
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        commits = response.json()
        if commits:
            last_commit = commits[0]
            last_modified_time_str = last_commit['commit']['committer']['date']
            if last_modified_time_str.endswith('Z'):
                last_modified_time_str = last_modified_time_str[:-1]
            last_modified_time = datetime.fromisoformat(last_modified_time_str)
            if last_config_update_time is None or last_modified_time > last_config_update_time:
                last_config_update_time = current_time
                last_config_update_time_str = last_config_update_time.isoformat()
                alternate_config_path = get_alternate_config_file()
                alternate_config = build_file_path(alternate_config_path)
                raw_response = requests.get('https://raw.githubusercontent.com/kongji1/go/main/go.txt')
                raw_response.raise_for_status()
                with open(alternate_config, 'w', encoding='utf-8') as file:
                    file.write(raw_response.text)
                status_manager.update_status('last_config_update_time', last_config_update_time_str)
                logger.info(f"{alternate_config} 已成功更新。尝试执行新配置。")
                execute_config_file(alternate_config)
                current_config = switch_config()
                logger.info(f"现在使用配置文件: {get_config_file()}")
            else:
                logger.info("文件未更新，无需执行操作。")
        else:
            logger.error("未能找到更新时间标签。")
    except Exception as e:
        logger.error(f"更新或执行加载配置时出错: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

async def schedule_config_updates():
    while True:
        fetch_and_update_config()
        logger.info(f"暂停8, 14400")
        await asyncio.sleep(14400)

def get_max_limit():
    try:
        max_limit = max(len(kline_data_cache.get(interval_5m, [])),
                        len(kline_data_cache.get(interval_ssbb_3m, [])),
                        limit, limit_test)
        logger.info(f"最大 limit calculated: {max_limit}")
        return max_limit
    except Exception as e:
        logger.info(f"get_max_limit 出错: {e}")
        logger.debug(f"堆栈跟踪: {traceback.format_exc()}")

async def run_periodic_tasks():
    global last_config_update_time, current_time, mfi_trigger_low_5m, mfi_trigger_high_5m
    last_config_update_time = price_to_datetime(last_config_update_time_str)
    while True:
        try:
            current_time = datetime.now()
            if current_time.minute % 2 == 1:
                await fetch_and_update_config()
            if current_time.hour == 0 and current_time.minute == 2:
                max_limit = get_max_limit()
                await cache_kline_data(symbol, [interval_5m, interval_ssbb_3m], max_limit)
                logger.info("异步开始执行技术指标回测任务")
                rsi_trigger_low_5m, rsi_trigger_high_5m, _ = await rsibest(interval_5m)
                status_manager.update_status('rsi_trigger_low_5m', rsi_trigger_low_5m)
                status_manager.update_status('rsi_trigger_high_5m', rsi_trigger_high_5m)
                mfi_trigger_low_5m, mfi_trigger_high_5m, _ = await mfibest(interval_5m)
                logger.info(f"更新后的 MFI 触发点: {mfi_trigger_low_5m}/{mfi_trigger_high_5m}")
                status_manager.update_status('mfi_trigger_low_5m', mfi_trigger_low_5m)
                status_manager.update_status('mfi_trigger_high_5m', mfi_trigger_high_5m)
                so_trigger_low_5m, so_trigger_high_5m, _ = await sobest(interval_5m)
                status_manager.update_status('so_trigger_low_5m', so_trigger_low_5m)
                status_manager.update_status('so_trigger_high_5m', so_trigger_high_5m)
        except Exception as e:
            logger.error(f"在运行周期性任务时出现错误: {e}")
            traceback.print_exc()
            logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        finally:
            logger.info(f"暂停8, {60}")
            await asyncio.sleep(60)

async def main_loop():
    """
    主循环：不断更新市场数据、执行配置加载、下单逻辑及持仓更新。
    """
    global intervals, current_price, ssbb, sbsb, last_price_update_time
    start_time = datetime.now()
    last_price_update_time = price_to_datetime(last_price_update_time_str)
    start_price, last_price_update_time = get_current_price(symbol)
    logger.info(f"开始价格: {start_price}")

    intervals = ['3m', '5m']
    # 如需更多时间周期，可调整 intervals 列表
    await cache_kline_data(symbol, intervals, limit_test)
    fetch_and_update_active_orders(symbol)  # 加载订单记录
    current_price = start_price

    while True:
        try:
            logger.info("\n******* 正在执行25主循环 *******")
            execute_config_file(get_config_file())  # 动态加载配置
            get_current_time()
            if current_time - start_time >= timedelta(minutes=max_run_time):
                logger.info("主循环运行超过最大时长，现在停止。")
                break
            await update_kline_data_async(symbol, current_time)
            current_price, last_price_update_time = get_current_price(symbol)
            if trading_strategy_enabled == 1:
                trading_strategy()
            ssbb, sbsb = calculate_composite_score(current_price, last_order_price,
                                                   last_s_order_price,
                                                   stop_loss_limit,
                                                   take_profit_limit)
            # 如需调试，可取消下面代码注释打印详细信息
            # logger.info(f"配置加载后 sbsb: {sbsb}, ssbb: {ssbb}\n")
            if sbsb == 1:
                current_price, last_price_update_time = get_current_price(symbol)
                order_position = 'lb' if ssbb == 1 else 'ss'
                order_price, origQty = calculate_next_order_parameters(current_price, leverage, order_position)
                # 以下两行任选其一（例如使用 'QUEUE_5' 标识下单价格）
                response = place_limit_order(symbol, order_position, 'QUEUE_5', origQty, 100 * Slippage)
                if response:
                    update_order_status(response)
                    logger.info("网格系统下单成功")
            current_status()
            # beta()  # 测试代码（可取消注释用于测试）
            logger.info(f"暂停9, {monitoring_interval}")
            await asyncio.sleep(monitoring_interval)  # 暂停一定时间后继续循环
            if current_time.minute % 2 == 1:
                run_powershell_script('112.ps1')
            status_manager.update_status('Last_heartbeat_time', current_time)
        except ClientError as error:
            logger.error(f"主循环运行时错误: {error}")
            logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"主循环运行时发生错误: {e}")
            traceback.print_exc()
            logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
            logger.info(f"暂停10, {monitoring_interval}")
            await asyncio.sleep(monitoring_interval)

def beta():
    """
    测试函数 beta：检测是否存在指定 PowerShell 脚本文件，不存在则进行测试。
    """
    script_path = os.path.join(BASE_PATH, '112.ps1')
    if not os.path.isfile(script_path):
        logger.info(f"脚本文件 {script_path} 不存在，开始测试")
        position = 'lb'
        price1, _ = get_current_price(symbol)
        price = (price1 * 0.8) if position == 'lb' else (price1 * 1.2)
        quantitya = min_quantity
        callback = 0.1
        response = client.commission_rate(symbol=symbol)
        print(response)
    else:
        logger.info(f"脚本文件 {script_path} 存在，终止测试")
        return
    logger.info("\n/nbbbbbeeeeettttttaaaaa")

# =============================================================================
# 程序入口及主循环启动
# =============================================================================

logger = None

async def main():
    """
    主入口函数：同时启动周期性任务和主循环任务。
    """
    task1 = asyncio.create_task(run_periodic_tasks())  # 创建周期性任务
    task2 = asyncio.create_task(main_loop())           # 创建主交易循环任务
    await asyncio.gather(task1, task2)                   # 同时运行两个任务

def run_main_loop():
    """
    主程序循环：
    初始化日志、加载配置、创建交易客户端，启动异步主循环，
    如遇异常则等待后重启程序。
    """
    global logger, client
    while True:
        try:
            logger = setup_logging()
            execute_config_file(get_config_file())
            config = load_config()
            client = setup_um_futures_client()
            get_public_ip_and_latency()
            asyncio.run(main())
            logger.info(f"暂停11, {30}")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("检测到键盘中断。程序将在 60 秒后重新启动...")
            time.sleep(60)
            os.execv(sys.executable, ['python'] + sys.argv)
        except Exception as e:
            logger.error(f"程序运行中发生错误: {e}")
            traceback.print_exc()
            logger.debug(f"堆栈跟踪: {traceback.format_exc()}")
            logger.info(f"暂停12, {30}")
            time.sleep(30)
            os.execv(sys.executable, ['python'] + sys.argv)


if __name__ == "__main__":
    run_main_loop()
