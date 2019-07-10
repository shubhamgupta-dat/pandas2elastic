# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 20:05:13 2018

@author: shubhamgupta-dat
"""

import json
import math
from tqdm import *
import pandas as pd
from hashlib import sha256
from elasticsearch.helpers import bulk
import elasticsearch
from ssl import create_default_context

class ElasticSearchAdapter():
    """
    This class takes the hostname and port perform all major operation required for 
    dataframe operations
    """
    
    def __init__(self, hosts = list(),                
                 username = None,
                 passkey = None,
                 port = 9200,
                 ca_cert_filepath=None):
        """
        Params:
            host: List of Multiple Elastic Nodes. Currently focuses on one node
            username: Username, if authentication is enabled on Elastic Nodes
            passkey: Passkey used for the username
        """
        self.hostnames = hosts
        self.es = None
        self.port = port
        self.username = username
        self.__passkey__ = passkey
        self.context = create_default_context(cafile=ca_cert_filepath)
        
        if(username==None):
            self.es = elasticsearch.Elasticsearch(hosts=self.hostnames,verify_certs=False)
        else:            
            self.es = elasticsearch.Elasticsearch(hosts=self.hostnames,
                                                        http_auth=(self.username,self.__passkey__),
                                                        scheme="https",
                                                        port=self.port,
                                                        ssl_context=self.context)
        return
    
    def __generate_id__(self,row,id_fields=[]):
            return sha256('+'.join(
                            [str(row[field]) 
                            for field in id_fields]).encode()
                            ).hexdigest()
    
    def put_dataframe(self,index,dataframe,create_id=False,id_fields=[]):
        """
        Insert Data into an existing index.
        
        """
        if self.__index_exist__(index)==False:
            print("Index:{0} does not exist in elasticsearch indices.".format(index))
            return
        for i in tqdm(range(dataframe.shape[0])):
            data=self.prune_records(dataframe.iloc[i].to_json(date_format='iso'))
            
            id_val = None
            if create_id:
                id_val = self.__generate_id__(dataframe.iloc[i])
            
            self.es.index(index=index,doc_type='data',id=id_val, body=data)
        return
    
    def insert_bulk(self,index,dataframe,create_id=False,id_fields=[]):
        """
        Insert Bulk Data into an existing index.
        
        """
        if create_id:
            data = ({
                    "_index": index,
                    "_type" : 'data',
                    "_id"   :  self.__generate_id__(row,id_fields=id_fields),
                    "_source": self.prune_records(row.to_json(date_format='iso')),
                 } for idx, row in tqdm(dataframe.iterrows()))
        else:
            data = ({
                    "_index": index,
                    "_type" : 'data',
                    "_source": self.prune_records(row.to_json(date_format='iso')),
                 } for idx, row in tqdm(dataframe.iterrows()))
        
        return bulk(self.es, data)
            
     
    def __index_exist__(self,index):
        """
        Check if the index exist in the given Elasticsearch Stash. Need deprication
        or update after multiple hosts.
        """
        try:
            return self.es.indices.exists(index)
        except ValueError:
            raise("Not Authorized to Check such Index")
            return False
        
    def generate_settings_mapping(self,dataframe,index,doc_type,aggregators=[],shards=1,replicas=0):
        mapping = {}
        
        settings = {"number_of_shards":int(shards),
                   "number_of_replicas":int(replicas)
                    }
                                   
        for col in dataframe.columns:
            d_type = str(dataframe.dtypes[col])
            if d_type in ['int64','int8']:
                mapping[col] = {"type":'integer'}
            elif d_type in ['float32','float64']:
                mapping[col] = {"type":'float'}
            elif d_type == 'bool':
                mapping[col] ={"type":'boolean'}
            elif d_type in ['datetime64[ns]', 'timedate64[ns]']:
                mapping[col] ={"type":'date'}#, 'format': 'dateOptionalTime'
            elif col in aggregators:
                mapping[col] = {"type":"keyword"}
            elif d_type=='object':
                mapping[col] = {"type":'text'}
            else:
                mapping[col]={"type":'text'}
            final_map={"settings":settings,
                       "mappings":
                           {doc_type:
                               {"properties":mapping}
                           }
                      }
        print("Final Mapping for index:",index)
        print("===============================")
        #print(final_map)
        return final_map

    def create_index_from_df(self, index, dataframe, aggregators=[], persist_index=True,shards = 1, replicas = 0):
        """
        This method creates index in elasticsearch indices for particular dataframe.
        """
        try:
            final_map = self.generate_settings_mapping(dataframe,index,'data',
                                                       aggregators=aggregators,
                                                       shards=shards,replicas=0)
            if persist_index:
                self.es.indices.create(index=index,ignore=400,body=final_map)
                
            print("Created Index")
            return final_map
        except:
            raise Exception("Index could not be created. Checked Mapping")

    def delete_index(self,index):
        if self.__index_exist__(index):
            self.es.indices.delete(index=index, ignore=[400, 404])
            print ("Index Deleted")
        else:
            print ("Index does not exist.")
        return
    
    def prune_records(self,rec):
        """
        This method removes keys which do not contain value
        """
        return {k: v for k, v in json.loads(rec).items() if not (v is None or (isinstance(v, list) and sum(pd.isnull(v) > 0)) or
                                                    (isinstance(v, float) and math.isnan(v))
                                                    or isinstance(v, pd.tslib.NaTType))}
