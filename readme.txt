(1)��Ҫ64λoracle�ͻ���֧��

��E:\Oracle\instantclient_11_2��ӵ�ϵͳ����PATH��
NLS_LANG=SIMPLIFIED CHINESE_CHINA.UTF8   -->> ���������ַ���
TNS_ADMIN=E:\Oracle\instantclient_11_2       -->> ָ��tnsnames.ora����λ��

(2)64λpython��װ��������python�汾3.5

conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/main/
conda config --set show_channel_urls yes

conda update python

python -m pip install -U pip

(3)��װ����python���⻷��


pip install pqi -i https://pypi.tuna.tsinghua.edu.cn/simple

(//�о�����֧�ֵ�PyPiԴ
pqi ls
//�ı� PyPiԴ
pqi use <name>
//��ʾ��ǰPyPiԴ
pqi show
//�Ƴ�pipԴ
pqi remove <name>
//����µ�pipԴ
pqi add <name> <url>
//���������°� pqi
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



(4)��װ�쳣Ԥ������
python SCAbWarning.py install
\
python AbWarningtest.py install