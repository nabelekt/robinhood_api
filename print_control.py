import os
import sys

class Controller:
  def __init__(self):
    self.original_out = sys.stdout

  def disable_printing(self):
    f = open(os.devnull, 'w')
    sys.stdout = f

  def enable_printing(self):
    sys.stdout = self.original_out
