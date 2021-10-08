from flask import Flask

# 创建app应用，__name__是python预定义变量，被设置为使用本模块
app = Flask(__name__)
# 如果你使用的IDE，在routes这里会报错，因为还没有创建
from app import routes
