from   saga.engine.logger import getLogger
from   saga.engine.engine import getEngine, ANY_ADAPTOR

import saga.exceptions
import saga.attributes
import saga.task

from saga.filesystem import READ, START

class Directory (object) :

  def get_size  (self, name, flags=None,  ttype=None) : pass 
  #   name:     saga.Url
  #   flags:    saga.namespace.flags enum
  #   ttype:    saga.task.type enum
  #   ret:      int / saga.Task

  def is_file   (self, name,              ttype=None) : pass 
  #   name:     saga.Url
  #   ttype:    saga.task.type enum
  #   ret:      bool / saga.Task

  def open_dir  (self, name, flags=READ,  ttype=None) : pass 
  #   name:     saga.Url
  #   flags:    saga.namespace.flags enum
  #   ttype:    saga.task.type enum
  #   ret:      saga.filesystem.Directory / saga.Task

  def open      (self, name, flags=READ,  ttype=None) : pass 
  #   name:     saga.Url
  #   flags:    saga.namespace.flags enum
  #   ttype:    saga.task.type enum
  #   ret:      saga.filesystem.File / saga.Task


