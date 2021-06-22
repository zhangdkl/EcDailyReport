(1)需要64位oracle客户端支持

将E:\Oracle\instantclient_11_2添加到系统变量PATH中
NLS_LANG=SIMPLIFIED CHINESE_CHINA.UTF8   -->> 服务器端字符集
TNS_ADMIN=E:\Oracle\instantclient_11_2       -->> 指定tnsnames.ora所在位置

(2)64位python安装，升级，python版本3.5

conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/main/
conda config --set show_channel_urls yes

conda update python

python -m pip install -U pip

(3)安装运行python虚拟环境


pip install pqi -i https://pypi.tuna.tsinghua.edu.cn/simple

(//列举所有支持的PyPi源
pqi ls
//改变 PyPi源
pqi use <name>
//显示当前PyPi源
pqi show
//移除pip源
pqi remove <name>
//添加新的pip源
pqi add <name> <url>
//升级到最新版 pqi
pip install --upgrade pqi
)

pqi use aliyun

# pip install virtualenv -i https://pypi.tuna.tsinghua.edu.cn/simple
# pip install virtualenvwrapper  -i https://pypi.tuna.tsinghua.edu.cn/simple

# virtualenv venv

# pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

pip install numpy
pip install pandas
pip install pymssql
pip install cx_Oracle
pip install sqlalchemy
pip install apscheduler

pip install requests



(4)安装异常预警服务
python SCAbWarning.py install
\
python AbWarningtest.py install