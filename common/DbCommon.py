# -*- coding: utf-8 -*-
import os
import time
import traceback
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import logging.config
from DBUtils.PooledDB import PooledDB

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# logfile = os.path.join(settings.get_work_path(), 'dataprocess/oracleprocess/mes/log/mes.log')
# fh = logging.FileHandler(logfile, mode='a')
# fh.setLevel(logging.DEBUG)
# formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
# fh.setFormatter(formatter)
# logger.addHandler(fh)

class mysql2pd(object):
    def __init__(self, host, port, db, user, pwd, retry_num=3, env_lang='utf8'):
        '''
        :param host: 主机ip
        :param port: 端口号
        :param db: 数据库
        :param user: 用户名
        :param pwd: 密码
        '''
        for i in range(1, retry_num+1):
            try:
                self.pool = self.connnect(host, int(port), user, pwd, db, env_lang)
                break
            except Exception as e:
                logger.warning('Retry get mysql connection , the [%d] times, err %s' % (i, e.message))
                if i == retry_num:
                    logger.warning(traceback.format_exc())
                    raise e
                time.sleep(10)
        self.db=db
        self.user=user
        self.host=host
        self.pwd=pwd
        self.port=port

    # may raise exception
    def connnect(self, host, port, user, pwd, db, charset='utf8'):
        pool = PooledDB(pymysql, 5, host=host, port=int(port), user=user, passwd=pwd, db=db, charset=charset)
        return pool

    def close(self):
        self.pool.close()

    def doget(self,sql):
        '''
        用于执行查询sql语句
        :param sql: 查询语句
        :return: dataframe形式的查询结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        try:
            res = pd.read_sql(sql,self.conn)
        except Exception as e:
            logger.info(u"执行失败：" + sql + "\n" + u"失败原因：")
            logger.info(e)
            raise e

        return res

    def dopost(self,sql):
        '''
        用于执行对数据库改动的sql语句
        :param sql: 增删改的sql语句
        :return: 执行结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        res=False
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            res=True
            print(u"执行成功："+sql)
            logger.info(u"执行成功："+sql)
        except Exception as e:
            # 错误回滚
            print(u"执行失败："+sql)
            print(u"失败原因：")
            print(e)
            logger.info(u"执行失败："+sql+"\n"+u"失败原因：")
            logger.info(e)
            self.conn.rollback()
        return res

    def showtables(self,keyword=None,showpars=False):
        '''
         显示数据库中的表
         :param keyword: 表名关键词
         :param showpars: 是否显示表的所有信息，若为否则只显示表名
         :return: 查询结果
         '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        if not showpars:obj='table_name'
        else:obj='*'
        sql="select "+obj+" from information_schema.tables"
        if keyword:
            sql+=" where table_name like '%"+keyword+"%'"
        try:
            res = pd.read_sql(sql, self.conn)
        except Exception as e:
            logger.info(u"执行失败：" + sql + "\n" + u"失败原因：")
            logger.info(e)
            raise e
        return res

    def getdata(self,table,pars=None,tjs=None,blimit=None,elimit=None):
        '''
        从数据库中取出数据放到dataframe中
        :param table: 数据源表
        :param pars: list类型，列出想要提取的字段名，若为空则查询所有字段
        :param blimit: 数据行数最小值限制
        :param elimit: 数据行数最大值限制
        :return: dataframe类型查询结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        if pars==None:
            items='*'
        else:
            items=','.join(pars)
        sql1='select '+items+' from '+table
        if blimit!=None or elimit!=None:
            sql1+=' limit '
            if blimit!=None and elimit!=None:sql1+=str(int(blimit)-1)+','+str(int(elimit)-int(blimit))
            elif elimit!=None:sql1+=str(elimit)
            else:
                sql_count="select table_rows from information_schema.tables where table_name='"+table+"'"
                self.cursor.execute(sql_count)
                n=self.cursor.fetchone()[0]
                sql1 += str(int(blimit) - 1) + ',' + str(n- int(blimit))
        if tjs!=None:
            if sql1.find('where')!=-1:sql1=sql1.replace('where','where '+' and '.join(tjs)+'and')
            else:sql1+=' where '+' and '.join(tjs)
        # print("执行查询："+sql1)
        try:
            res = pd.read_sql(sql1, self.conn)
        except Exception as e:
            logger.info(u"执行失败：" + sql1 + "\n" + u"失败原因：")
            logger.info(e)
            raise e
        return res

    def addone(self,table,values,keys=None):
        '''
        增加一条记录
        :param table: 目标表
        :param values: list类型，要插入的值
        :param keys:list类型，字段名
        :return:执行结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        sql_insert = "insert into "+table+" "
        for i in range(0,len(values)):
            if not str(values[i]).isdigit() and values[i][0]!="'":
                values[i]="'"+str(values[i])+"'"
        if keys!=None:
            sql_insert+="("+",".join([str(key) for key in keys])+") "
        sql_insert+="values("+",".join([str(value) for value in values])+")"
        return self.dopost(sql_insert)
    def delete(self,table,find_dict):
        '''
        删除数据
        :param table: 目标表
        :param find_dict: where条件对，例如：{'=':[('部门','业务组'),('性别','男')],'like':[('name','%冯%')]}
        :return: 执行结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        sql="delete from "+table
        if find_dict!=None:
            tj = []
            for key in find_dict.keys():
                for kv in find_dict[key]:
                    if not str(kv[1]).isdigit():
                        kk = ("'" + str(kv[1]) + "'").replace("''", "'")
                    tj.append(str(kv[0]) + " "+key+ " "+kk)
            # for kv in find_dict['like']:
            #     kk = ("'" + str(kv[1]) + "'").replace("''", "'")
            #     tj.append(str(kv[0]) + " like " +kk)
            sql +=" where "+" and ".join(tj)
        return self.dopost(sql)
    def update(self,table,keyandvals,find_dict=None):
        '''
        更新数据
        :param table: 目标表
        :param keyandvals: 更新值，例如：{'age':'age-1','性别':'男'}
        :param find_dict: where条件对，例如：{'=':[('部门','业务组'),('性别','男')],'like':[('name','%冯%')]}
        :return:
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        sql="update "+table+" set "+",".join([str(k)+"="+str(v) for k,v in keyandvals.items()])
        if find_dict!=None:
            tj = []
            for key in find_dict.keys():
                for kv in find_dict[key]:
                    if not str(kv[1]).isdigit():
                        kk = ("'" + str(kv[1]) + "'").replace("''", "'")
                    tj.append(str(kv[0]) + " "+key+ " "+kk)
            # for kv in find_dict['like']:
            #     kk = ("'" + str(kv[1]) + "'").replace("''", "'")
            #     tj.append(str(kv[0]) + " like " +kk)
            sql +=" where "+" and ".join(tj)
        return self.dopost(sql)

    def addtable(self,table,pars):
        '''
        建表
        :param table: 表名
        :param pars: 字段属性，例如addtable("`xiaoming`",[("name","varchar(30)","not null"),("sex","varchar(30)"),("age","int(6)")])
        :return: 执行结果
        '''
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        if table[0]!="`":table="`"+table+"`"
        sql="CREATE TABLE IF NOT EXISTS "+table+"("+",".join([' '.join(x) for x in pars])+")ENGINE=InnoDB DEFAULT CHARSET=utf8"
        return self.dopost(sql)

    def write2mysql(self,dataframe,table):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        res=False
        try:
            engine = create_engine("mysql+pymysql://"+self.user+":"+self.pwd+"@"+self.host+":"+self.port+"/"+self.db+"?charset=utf8")
            dataframe.to_sql(name=table, con=engine, if_exists='append', index=False, index_label=False)
            res=True
        except Exception as e:
            print(e)
            logger.info(u"执行失败：write2mysql\n" + u"失败原因：")
            logger.info(e)
        return res
