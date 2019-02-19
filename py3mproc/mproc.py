#!/usr/bin/python3
"""
 Multi-Processing Framework 


"""

import os,sys,time,multiprocessing
import logging
import json
import py3toolbox as tb
import time
import random


def default_config() :
  config = {
            "default" : {
                "start_task"        : "__START_TASK__",
                "end_task"          : "__END_TASK__",
                "kill_task"         : "__KILL_TASK__",    
                "id_template"       : "#ID#",    
                "end_task_max"      : 4,
                "q_timeout"         : 120,
                "q_throttled_max"   : 9990,
                "end_task_qmin"     : 100,
                "max_errors"        : 0,
                "progress_bar_max"  : 80,
                "proc_status"       : {
                                        "null"            : " ",
                                        "started"         : "↑",
                                        "waiting"         : "_",
                                        "running"         : "►",
                                        "done"            : "√",
                                        "ended"           : "○",
                                        "exited"          : ".",                      
                                        "error"           : "!",
                                        "killed"          : "X"
                                      },
                "log_format"        : "{0:22} : {1:16} : {2:5} - {3}",
                "log_batch"         : 200,
                "log_levels"        : {
                                        "ERROR"           : 50,
                                        "INFO"            : 40,
                                        "DEBUG"           : 10
                                      }
             }
        }
  return config

"""
 -------------------------------------------------------------
 -  General Shared Function
 -------------------------------------------------------------
"""

def gen_log(comp, loglvl, logtxt, q_log, log_filter=None) :
  if log_filter == 'ERROR' and (loglvl == 'DEBUG' or loglvl == 'INFO') :  return
  if log_filter == 'INFO'  and loglvl == 'DEBUG'  :  return  
  
  log = {}
  log['timestamp'] = tb.get_timestamp(iso_format=True)
  log['component'] = comp
  log['log_level'] = loglvl
  log['log_text']  = logtxt
  q_log.put(log)

def push_q(q, message=None):
  if message is None : return
  config = default_config()
  q_throttled_max = config['default']['q_throttled_max']
  while True:
    if q.qsize() <= q_throttled_max :
      q.put(message)
      break
    else:
      time.sleep(1)
  
"""
 -------------------------------------------------------------
 -  Worker
 -------------------------------------------------------------
"""
class Worker(multiprocessing.Process):
  def __init__(self, config, idx, q_log , q_mgr):
    multiprocessing.Process.__init__(self)  
    self.config         = config
    self.name           = config['name']
    self.id             = idx
    self.u_name         = self.name + '_' + str(self.id)
    self.q_in           = self.config['q_in']
    self.q_out          = self.config['q_out']
    self.q_timeout      = self.config['q_timeout']
    self.q_log          = q_log
    self.q_mgr          = q_mgr
    self.task           = None
    self.task_status    = {}
    task_module         = __import__(self.config['task_module'])    
    self.exec_task      = eval('task_module.' + self.config['name'])
    self.start_task     = self.config['start_task'] + self.config['id_template'].replace('ID',str(self.id))    
    self.end_task       = self.config['end_task']   + self.config['id_template'].replace('ID',str(self.id))
    self.trigger_start  = self.config['trigger_start']
    self.max_errors     = self.config['max_errors']
    self.end_task_max   = self.config['end_task_max']
    self.end_task_qmin  = self.config['end_task_qmin']
    self.log_filter     = self.config['log_level']
    self.err_count      = 0
    self.tasks_count    = 0
    self.own_end_rcvd   = 0
    self.end_task_dic   = {}
    
    self.param          = {
                            'u_name'             : self.u_name, 
                            'last_call_required' : False, 
                            'last_call'          : False 
                          }
    self.task_exec_time = 0
    
 
    
  def get_task(self):
    task = None
    while True:
      try :  
        self.update_status('waiting')
        task = self.q_in.get(timeout=self.q_timeout)
        if task is not None:
          self.task = task
          break
        else:
          gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'task waiting timeout :' + str(self.q_timeout), q_log=self.q_log, log_filter=self.log_filter)
      except :
        pass    

  def execute_task(self) :
    if self.param['last_call'] == False: self.tasks_count +=1
    self.update_status('running')
    self.task_exec_time = tb.timer_start()
    self.param = self.exec_task(param=self.param, task=self.task, q_log=self.q_log, q_out=self.q_out)
    self.task_exec_time = int(tb.timer_check(self.task_exec_time) * 1000)/1000
    self.update_status('done')
    pass 
 
  def update_status(self, status):
    self.task_status =  { 
                          'name'        : self.name, 
                          'id'          : self.id , 
                          'status'      : status, 
                          'tasks_count' : self.tasks_count 
                        }
    if status == 'running' :
      gen_log(comp = self.u_name, loglvl = 'INFO', logtxt = status + ' - ' + self.u_name + '#' + str(self.tasks_count), q_log=self.q_log, log_filter=self.log_filter)
    elif  status == 'done' :
      gen_log(comp = self.u_name, loglvl = 'INFO', logtxt = status + ' - ' + self.u_name + '#' + str(self.tasks_count) + ', exec_time = ' + str(self.task_exec_time) + ' s', q_log=self.q_log, log_filter=self.log_filter)
    else :
      gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = status, q_log=self.q_log, log_filter=self.log_filter)
    
    if self.q_mgr.qsize() > 1000 : 
      if (status in [ "exited" , "started", "ended", "error" ] ) or (self.tasks_count % 20 == 0):
        self.q_mgr.put(self.task_status)
    else:
      self.q_mgr.put(self.task_status)
    
  def run(self):
    self.update_status('started')
    while True: 
      self.task           = None
      try:
        if self.q_in is not None:
          self.get_task()
          gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'TASK={' +  str(self.task)  + '}', q_log=self.q_log, log_filter=self.log_filter)
          
          # check trigger start run once
          if self.trigger_start == True and self.task == self.start_task :
            self.own_end_rcvd = 0
            self.end_task_dic = {}
            self.execute_task()
            gen_log(comp = self.u_name, loglvl = 'INFO', logtxt = 'trigger-start : execution completed', q_log=self.q_log, log_filter=self.log_filter)
            self.update_status('ended')
            break
          
          # exception, there should be no self.start_task coming in
          if self.trigger_start == False and self.task == self.start_task :
            gen_log(comp = self.u_name, loglvl = 'ERROR', logtxt = 'There should be no self.start_task coming in', q_log=self.q_log, log_filter=self.log_filter)
            continue
            
          # normal task coming in(looping to consume more)
          if self.config['end_task'] not in self.task :
            self.own_end_rcvd = 0
            self.end_task_dic = {}
            self.execute_task()
            gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'regular task completed, move to next ...', q_log=self.q_log, log_filter=self.log_filter)
            continue

          # other workers end task coming in, the pass on
          # if self.config['end_task'] in self.task and self.task != self.end_task :
          if self.config['end_task'] in self.task :
            if self.task in self.end_task_dic and self.q_in.qsize() <= self.end_task_qmin :
              self.end_task_dic[self.task] +=1
            else:
              self.end_task_dic[self.task] =1
            
            if self.end_task_dic[self.task] < self.end_task_max :  
              # if receives other workers end-task, and NOT exceeds limit, pass on
              time.sleep(random.uniform(0, 1))
              self.q_in.put(self.task)
              time.sleep(random.uniform(0, 1))
              gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'others end-task, pass on ... ', q_log=self.q_log, log_filter=self.log_filter)
              continue            
            else : 
            # if receives other workers end-task, if reached limit, focrce end this worker
            
              # last call has no 'task' passed in, just execute to close off remaining tasks
              if self.param['last_call_required'] == True:
                self.param['last_call'] = True
                gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'last_call task to be executed ...', q_log=self.q_log, log_filter=self.log_filter)
                self.execute_task()  
                gen_log(comp = self.u_name, loglvl = 'INFO',  logtxt = 'last_call task completed', q_log=self.q_log, log_filter=self.log_filter)
              
              # if no last call required, 
              gen_log(comp = self.u_name,   loglvl = 'DEBUG', logtxt = 'others end-task : ' + str(self.end_task_dic[self.task])+ '/' + str(self.end_task_max) + ' : to be ended', q_log=self.q_log, log_filter=self.log_filter)
              self.update_status('ended')
              break

        else :
        # No start trigger required, start task immediately
          gen_log(comp = self.u_name, loglvl = 'INFO', logtxt = 'trigger-start : n/a, started immediately', q_log=self.q_log, log_filter=self.log_filter)
          self.execute_task()
          self.update_status('ended')
          break
      except KeyboardInterrupt:
        gen_log(comp = self.u_name, loglvl = 'INFO', logtxt =  'CTL-C Interrupted!', q_log=self.q_log, log_filter=self.log_filter)
        self.q_mgr.put(self.config['kill_task'])
        break          
      except Exception as err: 
        self.err_count +=1
        self.update_status('error')
        gen_log(comp = self.u_name, loglvl = 'ERROR', logtxt = 'task:' + str(self.task) + ':' + str(err), q_log=self.q_log, log_filter=self.log_filter)
        if  self.trigger_start == False and self.task is not None: 
          self.q_in.put(self.task)
        else:
          self.update_status('killed')
          break
        
          
"""
 -------------------------------------------------------------
 -  Monitor 
 -------------------------------------------------------------
"""

class Monitor(multiprocessing.Process):        
  def __init__(self, config, q_mtr , q_log,  q_mgr  ):    
    multiprocessing.Process.__init__(self)  
    self.config         = config
    self.q_mtr          = q_mtr
    self.q_mgr          = q_mgr
    self.q_log          = q_log
    self.u_name         = 'Monitor'
    self.end_task_max   = self.config['default']['end_task_max']
    self.end_task_count = 0
    self.q_timeout      = 1
    self.log_filter     = self.config['logger']['log_level']
    self.wf_status      = None
    self.last_wf_status = None
    self.start_time     = tb.timer_start()
    self.procs_max      = 1
    self.top_banner     = ''
    self.bottom_banner  = ''
    self.init_monitor()
    pass

  def init_monitor(self):
    self.top_banner   = '        {0:<20}'.format(tb.render('[%BRIGHT|MAGENTA_BG:  ' + self.config['title'] + '  %]', align='<', width =15) )
    bottom_banner_str = ' {0:>10} : {1:1} {2:>10} : {3:1} {4:>10} : {5:1} {6:>10} : {7:1}'
    
    self.bottom_banner += bottom_banner_str.format(
      tb.render('[%BLUE_BG|LYELLOW: STARTED %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['started'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: WAITING %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['waiting'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: RUNNING %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['running'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW:  DONE   %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['done'] + '%]', align='<', width=1)) + '\n'
    self.bottom_banner += bottom_banner_str.format(
      tb.render('[%BLUE_BG|LYELLOW:  ENDED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['ended'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: EXITED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['exited'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW:  ERROR  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['error'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: KILLED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['killed'] + '%]', align='<', width=1))+ '\n'

    
  def map_status (self, status):
    mapped_status = list (status)
    if (len(mapped_status)> self.procs_max) : self.procs_max = len(mapped_status)
    for i in range(len(mapped_status)) :
      mapped_status[i] = self.config ['default']['proc_status'][mapped_status[i]]
    return "".join(mapped_status)

  def get_message(self):
    try :  
      message = self.q_mtr.get(timeout=self.q_timeout)
      return message
    except : 
      return None

  def get_qsize(self, q_step):
    for step in self.config['workflow']['steps']:
      name  = step['name']
      if q_step == name : 
        if step['q_in'] is None : return 0
        return step['q_in'].qsize()
    return 0

  def refresh (self,actual_interval):
    if self.procs_max <= 10 : self.procs_max = 10
	
    format_str = '  {0:>4} - {1:<12} : {2:>8} > [{3:<' + str(self.procs_max) + '}] > {4:>8} ~ {5:>6}|{6:>5}   '
    output = []
    output_str = ''
    empty_title = format_str.format('','','','','','','')
    header = format_str.format(
      'Seq', 
      tb.render('[%LMAGENTA:Step%]',align='<',width=12), 
      tb.render('[%LRED:Pending%]',align='>',width=8), 
      tb.render('[%LCYAN:Workers%]',align='<',width=self.procs_max), 
      tb.render('[%LGREEN:Done%]',align='>',width=8), 
      'Rate',
      'Avg/s'
    )
	
    output.append ("")
    output.append (self.top_banner)
    output.append (tb.render('[%LYELLOW:' + '=' * len(empty_title) + '%]'))
    output.append (header)
    output.append (tb.render('[%LBLUE:' + '-' * len(empty_title) + '%]'))  
	
    # Keep track of last status, then calculate summary
    if self.last_wf_status is None :
      self.last_wf_status = self.wf_status

    for idx in range(len(self.wf_status.keys())):  
      for step in  self.wf_status.keys() :
        if self.wf_status[step]['seq'] == (idx + 1) :
          rate_now = int((self.wf_status[step]['tasks_count_sum'] - self.last_wf_status[step]['tasks_count_sum'])/actual_interval)
          rate_avg = int((self.wf_status[step]['tasks_count_sum'] / tb.timer_check(self.start_time)))
          _workers_status = self.map_status(self.wf_status[step]['workers'])
          _last_workers_status = self.map_status(self.last_wf_status[step]['workers'])
          _seq     = self.wf_status[step]['seq']
          # change color when all worker processes completed.
          # - prevent incorrect FIFO sequence
          if _last_workers_status == '.' * len(_workers_status): _workers_status = _last_workers_status
          if _workers_status != '.' * len(_workers_status) :
            _step   =  tb.render('[%LMAGENTA:'  + step + '%]',align='<'  ,width=12)
            _workers = tb.render('[%LCYAN:'     + _workers_status + '%]',align='<'  ,width=self.procs_max)
          else:
            _step   =  tb.render('[%LGRAY:'     + step + '%]',align='<'  ,width=12)
            _workers = tb.render('[%LGRAY:'     + _workers_status + '%]',align='<'  ,width=self.procs_max)
            
          _pending = tb.render('[%LRED:'      + str(self.get_qsize(step))                       + '%]',align='>'  ,width=8)
          _done    = tb.render('[%LGREEN:'    + str(self.wf_status[step]['tasks_count_sum'])    + '%]',align='>'  ,width=8)
          output.append (format_str.format(_seq, _step ,_pending, _workers, _done , rate_now, rate_avg))
          break
    output.append (tb.render('[%LBLUE:' + '-' * len(empty_title) + '%]'))     
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format ('*', tb.render('[%BRIGHT|GREEN_BG: Logger  %]',align='<',width=12), tb.render('[%LCYAN:' + str(self.q_log.qsize()) + '%]', align='>', width=11)  ))
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format ('*', tb.render('[%BRIGHT|GREEN_BG: Monitor %]',align='<',width=12), tb.render('[%LCYAN:' + str(self.q_mtr.qsize()) + '%]', align='>', width=11)  ))
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format ('*', tb.render('[%BRIGHT|GREEN_BG: Manager %]',align='<',width=12), tb.render('[%LCYAN:' + str(self.q_mgr.qsize()) + '%]', align='>', width=11)  ))
    output.append(tb.render('[%LYELLOW:' + '=' * len(empty_title) + '%]'))
    output.append(self.bottom_banner)
    output.append ('Time taken : ' + str ( int (tb.timer_check(self.start_time) * 1000 ) /1000 ) + ' s \n') 
    output_str = '\n'.join(output)

    if self.config ['monitor']['refresh'] == True:  tb.cls()
    print (output_str)
      

  def run(self): 
    print ("Monitor started")
    now = tb.timer_start()
    while True:
      try :
        message = self.get_message()
        if message is None : continue
        if message  == self.config['default']['end_task']:  
          self.end_task_count +=1
          self.refresh(actual_interval=tb.timer_check(now))
          if self.end_task_count >= self.end_task_max:
            break
          else:
            self.q_mtr.put(self.config['default']['end_task'])
            time.sleep(random.uniform(0, 1))
            continue
        else:
          end_task_count  = 0
          self.wf_status = message
          if (tb.timer_check(now)) >= self.config ['monitor']['refresh_interval']  : 
            if self.wf_status is None: continue
            self.refresh(actual_interval=tb.timer_check(now))
            self.last_wf_status = self.wf_status
            now = tb.timer_start()
            
      except KeyboardInterrupt:
        gen_log(comp = self.u_name, loglvl = 'INFO', logtxt =  'CTL-C Interrupted!', q_log=self.q_log, log_filter=self.log_filter)
        self.q_mgr.put(self.config ['default']['kill_task'])
      except Exception as err:
        gen_log(comp = self.u_name, loglvl = 'ERROR', logtxt = err, q_log=self.q_log, log_filter=self.log_filter)
    pass

"""
 -------------------------------------------------------------
 -  Logger
 -------------------------------------------------------------
"""

class Logger(multiprocessing.Process):        
  def __init__(self, config, q_mtr , q_log,  q_mgr  ):    
    multiprocessing.Process.__init__(self)  
    self.config         = config
    self.u_name         = 'Logger'
    self.q_mtr          = q_mtr
    self.q_log          = q_log
    self.q_mgr          = q_mgr
    self.q_timeout      = self.config['default']['q_timeout']
    self.end_task       = self.config['default']['end_task']
    self.end_task_max   = self.config['default']['end_task_max']
    self.own_end_rcvd   = 0
    self.log_file       = self.config['logger']['log_file']
    self.log_level      = self.config['default']['log_levels'][self.config['logger']['log_level']]
    self.log_format     = self.config['default']['log_format']
    self.log_batch      = self.config['default']['log_batch']
    self.log_messages   = []
    self.init_log_file()
 
  def init_log_file(self):
    tb.rm_file (self.log_file)
    
  def get_message(self):
    while True:
      try :  
        message = self.q_log.get(timeout=self.q_timeout)
        return message
      except : 
        pass
     
  def log_it(self, log_messages):
    log_formatted = ''
    for log_message in log_messages:
      timestamp     = log_message['timestamp']
      component     = log_message['component']
      log_text      = log_message['log_text']
      log_formatted = log_formatted + tb.format_str(self.log_format, timestamp, component, log_message['log_level'], log_text ) + '\n'

    if log_formatted != '' : 
      tb.write_log(file_name=self.log_file, text=log_formatted)
    return
  
  def run(self): 
    print ("Logger started")
    while True:
      message = self.get_message()

      # normal log msg coming in(looping to consume more)
      if message != self.end_task :
        self.own_end_rcvd = 0
       
        if 'log_level' in message :
          if self.config['default']['log_levels'][message['log_level']] < self.log_level : 
            continue      
        
        self.log_messages.append(message)
        # When error, write log immediately 
        if len(self.log_messages) >= self.log_batch or message['log_level'] == 'ERROR':
          self.log_it(self.log_messages)    
          self.log_messages = []
        continue

      # in case of timeout, message = None
      if message is None:
        gen_log(comp = self.u_name, loglvl = 'DEBUG', logtxt = 'Log message timeout: ' + str(self.q_timeout), q_log=self.q_log)
        self.log_it(self.log_messages)
        continue        

      # own end task coming in and exceed max limit         
      if message == self.end_task and self.own_end_rcvd >= self.end_task_max  : 
        self.log_it(self.log_messages)    
        break
        
      # own end task coming in            
      if message == self.end_task and self.own_end_rcvd < self.end_task_max  : 
        self.own_end_rcvd +=1
        self.q_log.put(self.end_task)
        continue      
        
    pass    


"""
 -------------------------------------------------------------
 -  Manager
 -------------------------------------------------------------
"""

class Manager(multiprocessing.Process):
  def __init__(self, wf_json=None, wf_config=None): 
    multiprocessing.Process.__init__(self)
    
    self.wf_config              = default_config()
    
    if wf_json is not None      :   self.wf_config.update(tb.load_json(wf_json))
    elif wf_config is not None  :   self.wf_config.update(wf_config)
    else :                          raise ValueError('Missing parameters for Manager()')

    self.u_name                 = 'Manager'
    self.q_mgr                  = multiprocessing.Queue()
    self.q_log                  = multiprocessing.Queue()  
    self.q_mtr                  = multiprocessing.Queue() 
    self.q_timeout              = self.wf_config['default']['q_timeout']
    self.log_filter             = self.wf_config['logger']['log_level']
    self.message                = None
    self.step_config            = {}
    self.step_workers           = {}
    self.wf_status              = {}
    self.wf_ended               = False

    return
    
    
  def get_message(self):
    try :  
      message = self.q_mgr.get(timeout=self.q_timeout)
      return message
    except : 
      return None

  def send_task_control_signal(self, q, number, signal):
    for _ in range(number): q.put(signal)

  def update_wf_status(self, message=None):
    index = 0
    
    if message is None:  
      # init workflow status
      for step in self.wf_config['workflow']['steps']:
        index +=1
        name = step['name']
        self.wf_status[name] = {}
        self.wf_status[name]['seq']               = index
        self.wf_status[name]['workers']           = ['null'] * self.step_config[name]['mproc_num']
        self.wf_status[name]['tasks_count']       = [0] * self.step_config[name]['mproc_num']
        self.wf_status[name]['tasks_count_sum']   = sum(self.wf_status[name]['tasks_count'])
    else :
      # update workflow status
      name = message['name']
      id   = message['id']
      self.wf_status[name]['workers'][id]         = message['status']
      self.wf_status[name]['tasks_count'][id]     = message['tasks_count']
      self.wf_status[name]['tasks_count_sum']     = sum(self.wf_status[name]['tasks_count'])

    return


  def clean_procs(self, step_name=None, terminate=True, join=True):
    if step_name is None:
      for step in self.wf_config['workflow']['steps']:
        name  = step['name']
        gen_log(comp = self.u_name, loglvl = 'DEBUG', log_filter= self.log_filter, logtxt = 'Teminationg processes [' + name + '] - ' + str(len(self.step_workers[name])) + ' ... ', q_log=self.q_log) 
        for w in self.step_workers[name] : 
          if terminate: w.terminate()
          if join : w.join()

      gen_log(comp = self.u_name, loglvl = 'DEBUG',  log_filter= self.log_filter, logtxt = 'Stopping Monitor and Logger', q_log=self.q_log) 
      time.sleep(10)
	  # turn off Logger and Monitor processes
      self.q_log.put (self.wf_config['default']['end_task'])
      self.q_mtr.put (self.wf_config['default']['end_task'])    
      self.monitor.join()      
      self.logger.join()
    else:
      name  = step_name
      gen_log(comp = self.u_name, loglvl = 'DEBUG',  log_filter= self.log_filter, logtxt = 'Teminationg processes [' + name + '] - ' + str(len(self.step_workers[name])) + ' ... ', q_log=self.q_log)  
      for w in self.step_workers[name] : 
        if terminate: w.terminate()
        if join : w.join()
      
    return 

  def start_wf(self):
	# ===================================
	# kick off workflow
	# ===================================
	
    # setup q_out and others
    for step in self.wf_config['workflow']['steps']:
      name = step['name']
      self.step_config[name] = step
      self.step_config[name]['q_out'] = multiprocessing.Queue()
      self.step_config[name]['task_module'] = self.wf_config['task_module']
      self.step_config[name].update(self.wf_config['default'])
      self.step_config[name].update(self.wf_config['logger'])
      self.step_config[name].update(self.wf_config['monitor'])
    
    # setup q_in, connect all steps by queues
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step["dependency"]
      if len(step["dependency"]) > 0:
        self.step_config[name]['q_in'] = self.step_config[dependency_name]['q_out']
      else :
        self.step_config[name]['q_in'] = None

    # start Logger & Monitor
    self.logger       = Logger (config = self.wf_config, q_mtr = self.q_mtr, q_log = self.q_log, q_mgr = self.q_mgr)
    self.monitor      = Monitor(config = self.wf_config, q_mtr = self.q_mtr, q_log = self.q_log, q_mgr = self.q_mgr)
    
    self.logger.start()
    self.monitor.start()
    time.sleep(10)
    self.update_wf_status()

    # start workers
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step["dependency"]
      if step['join_wait'] == False:    
        self.step_workers[name] = [ Worker(config=self.step_config[name], idx = i, q_log = self.q_log, q_mgr = self.q_mgr) for i in range(self.step_config[name]['mproc_num']) ]
        for w in self.step_workers[name] : w.start()

    return

  def set_step_exited(self, step_ended):
    # set wf step to be 'exited'
    index = 0
    seq = self.wf_status[step_ended]['seq']
    for step in self.wf_config['workflow']['steps']:
      index +=1
      step_name = step['name']
      if index <= seq :
        self.wf_status[step_name]['workers'] = ['exited'] * len(self.wf_status[step_name]['workers'])    
      else:
        break

  def is_wf_ended(self, step_ended):
    is_wf_ended  = True
    
    for step in self.wf_status.keys() :
      if set(self.wf_status[step]['workers']) != {'exited'} : is_wf_ended = False 
    self.wf_ended = is_wf_ended
    return self.wf_ended 

  def run_next_step(self, step_ended):
    # run next step
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step['dependency']
      if dependency_name != step_ended : continue
      
      # start listening for "join_wait=True" step
      if step['join_wait'] == True : 
        self.step_workers[name] = [ Worker(config=self.step_config[name], idx = i, q_log = self.q_log, q_mgr = self.q_mgr) for i in range(self.step_config[name]['mproc_num']) ]
        for w in self.step_workers[name] : w.start()
      
      # send start signal, this is specifically for task , who doesn't need prev task output as it's input, 
      # - only requires a singal to start   
      if step['trigger_start'] == True :  
        for id in range(self.step_config[name]['mproc_num']):
          start_signal = self.wf_config['default']['start_task'] +  self.wf_config['default']['id_template'].replace('ID',str(id))
          self.send_task_control_signal(q = self.step_config[name]['q_in'], number = 1, signal = start_signal)
      
      # Always send end signal, it should be at the LAST of the queues. ( however Python has FIFO issue for queues)
      for id in range(self.step_config[name]['mproc_num']):
        end_signal = self.wf_config['default']['end_task'] +  self.wf_config['default']['id_template'].replace('ID',str(id))
        self.send_task_control_signal(q = self.step_config[name]['q_in'], number = 1, signal = end_signal)

    pass

  def move_wf(self) :
    for step_name in self.wf_status.keys():
      if set(self.wf_status[step_name]['workers']) == {'ended'}:
        self.set_step_exited(step_name)
        if self.is_wf_ended (step_name) == False:   
          self.run_next_step(step_name)


  def run_wf_wrapper(self): 
    # start all processes (except join_wait=True)
    self.start_wf()
    while True:  
      self.message = self.get_message()
      if self.message is None : continue
      if self.message ==  self.wf_config['default']['kill_task'] :
        self.clean_procs()
        exit(1)
      self.update_wf_status(self.message)
      self.move_wf()
      self.q_mtr.put(self.wf_status)
      if self.wf_ended == True:  
        self.q_mtr.put(self.wf_status)
        self.close_wf()
        break

  def close_wf (self):
    gen_log(comp = self.u_name, loglvl = 'INFO', logtxt = 'All completed', q_log=self.q_log)
    self.clean_procs()

  def run(self): 
    tb.cls()
    try :
      self.run_wf_wrapper()
      return
    except KeyboardInterrupt:
      print('Manager CTL-C Interrupted!')   
      self.clean_procs() 
      return
