import os,sys,json,time,random,multiprocessing
import py3toolbox as tb
import py3mproc   as mp


import logging
import json
import random
import time

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]

def get_config() :
  config = {
    "task_module"       : MODULE_NAME,
    "title"				      : "Test Python Multiprocessing Workflow",
    "workflow" : {
      "steps" :   [
        {
          "dependency"    : "",
          "join_wait"     : False,    
          "trigger_start" : False,                             
          "name"          : "task1", 
          "mproc_num"     : 1
        },
        {
          "dependency"    : "task1",
          "join_wait"     : True,    
          "trigger_start" : False,                             
          "name"          : "task2", 
          "mproc_num"     : 1
        },
        {
          "dependency"    : "task2",
          "join_wait"     : True,    
          "trigger_start" : False,                             
          "name"          : "task3", 
          "mproc_num"     : 1
        },
        {
          "dependency"    : "task3",
          "join_wait"     : False,    
          "trigger_start" : False,                             
          "name"          : "task4", 
          "mproc_num"     : 4
        },
        {
          "dependency"    : "task4",
          "join_wait"     : False,    
          "trigger_start" : False,                             
          "name"          : "task5", 
          "mproc_num"     : 1
        },
        {
          "dependency"    : "task5",
          "join_wait"     : False,    
          "trigger_start" : False,                             
          "name"          : "task6", 
          "mproc_num"     : 4
        },    
        {
          "dependency"    : "task6",
          "join_wait"     : False,    
          "trigger_start" : False,                             
          "name"          : "task7", 
          "mproc_num"     : 20
        }
      ]
    },
            
    "monitor" : {
      "refresh"          : True,
      "refresh_interval" : 0.5
    },

    "logger" : {
      "log_file"      : "R:/TEMP1/1.log",
      "log_level"     : "DEBUG"
    }
  }
  return config


def task1(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  for _ in range(5) : 
    output( 'task')
    time.sleep (10)
  return param

def task2(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  if param['last_call'] == False :
    time.sleep (random.uniform(0, 0.1))
    for _ in range( int(40 * random.uniform(0, 1))) : 
      output( 'task')  
  else :
    tb.write_file ('C:/temp/aa.log', 'done')
  param['last_call_required'] = True    
  return param 

def task3(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  time.sleep (0.1 * random.uniform(0, 0.1))
  for _ in range( int(2 * random.uniform(0, 1))) : 
    output( 'task')   
  return param    

def task4(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  time.sleep (0.01 * random.uniform(0, 1))
  output( 'task')  
  return param     

def task5(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  time.sleep (0.01 * random.uniform(0, 0.1))
  output( 'task')  
  return param   

def task6(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  time.sleep (0.01 * random.uniform(0, 1))
  output( 'task')  
  return param       
  
def task7(param) :
  task, log, output = (param['task'], param['log_task_func'], param['output_task_func'])
  time.sleep (0.01 * random.uniform(0, 1))
  output( 'task')  
  return param 



if __name__ == "__main__":
  manager = mp.Manager(wf_config = get_config())
  manager.start()

