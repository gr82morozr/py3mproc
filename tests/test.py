#!/usr/bin/python3 -W ignore
import os,sys,time,multiprocessing

import py3mproc   as mp
import py3toolbox as tb
import logging
import json
import random
import time

def get_config() :
  config = {
            "task_module"       : "test",            
            "workflow" : {
              "steps" : [
                          {
                            "dependency"    : "",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task1", 
                            "mproc_num"     : 1
                          },
                          {
                            "dependency"    : "task1",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task2", 
                            "mproc_num"     : 8
                          } ,
                          {
                            "dependency"    : "task2",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task3", 
                            "mproc_num"     : 60
                          },
                          {
                            "dependency"    : "task3",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task4", 
                            "mproc_num"     : 8
                          },
                          {
                            "dependency"    : "task4",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task5", 
                            "mproc_num"     : 8
                          },
                          {
                            "dependency"    : "task5",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task6", 
                            "mproc_num"     : 8
                          },
                          {
                            "dependency"    : "task6",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task7", 
                            "mproc_num"     : 70
                          },
                          {
                            "dependency"    : "task7",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task8", 
                            "mproc_num"     : 7
                          },
                          {
                            "dependency"    : "task8",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task9", 
                            "mproc_num"     : 7
                          },
                          {
                            "dependency"    : "task9",
                            "join_wait"     : False,    
                            "trigger_start" : False,                             
                            "name"          : "task10", 
                            "mproc_num"     : 7
                          }
                        ]
            },
            
            "monitor" : {
                          "refresh"          : True,
                          "refresh_interval" : 0.5
            },

            "logger" : {
                  "log_file"      : "R:/TEMP/1.log",
                  "log_level"     : "DEBUG"
            }
          }
  return config


def task1(param, task=None, q_log=None, q_out=None) :
  for _ in range(20000) : 
    mp.push_q(q_out, 'task')
  return param

def task2(param, task=None, q_log=None, q_out=None) :
  if param['last_call'] == False :
    time.sleep (random.uniform(0, 0.1))
    for _ in range( int(40 * random.uniform(0, 1))) : 
      mp.push_q(q_out, 'task')  
  else :
    tb.write_file ('R:/aa.log', 'done')
  param['last_call_required'] = True    
  return param 

def task3(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.1 * random.uniform(0, 0.1))
  for _ in range( int(2 * random.uniform(0, 1))) : 
    mp.push_q(q_out, 'task')   
  return param    

def task4(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param     

def task5(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param   

def task6(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param       
  
def task7(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param 

def task8(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param     

def task9(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  mp.push_q(q_out, 'task')  
  return param    

def task10(param, task=None, q_log=None, q_out=None) :
  time.sleep (0.01 * random.uniform(0, 1))
  #mp.push_q(q_out, 'task')  
  return param    

if __name__ == "__main__":
  manager = mp.Manager(wf_config = get_config())
  manager.start()

