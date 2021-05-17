#!/usr/bin/env python3.7
# Last modified: 20200509 by Cheng-Tao Yang
# ctao2
# coding=utf-8

class hostmetadata:

    def __init__(self, arg_factory = None, arg_warehouse = None):
        import os, socket, platform, datetime, multiprocessing
        self.today      = datetime.datetime.today()
        self.home       = os.path.expanduser("~")
        self.callname   = __name__
        self.cwd        = os.getcwd()
        self.hostname   = socket.gethostname()
        self.platform   = platform.platform()[:10]        

        # factory directory, warehouse directory, cpu_code number
        self.factory    = self.home + '/pylily_factory' 
        self.warehouse  = self.home + '/pylily_warehouse' 
        self.cpu_code   = multiprocessing.cpu_count()

        # Machine list of CT Yang
        hostlist = {  
                      #for example: DESKTOP-kiki7 desktop Intel(R) Core(TM) i7-2600K CPU @3.40GHz 3.40GHz (code=4/m-threading=8)
                      ('r7920', 'Linux-3.10' ) :           #Dell R7920 
                      (
                            self.home + '/pylily_factory',
                            '/mnt/jupiter/NCREE_GIS',
                            50),

                      ('DESKTOP-TS700E9', 'Windows-10') :  #ASUS TS700E9 
                      (
                            'g:/pylily_factory',
                            'g:/NCREE_GIS',
                            40),
                            
                      ('m3', 'Windows-10') : 
                      (
                            self.home + '/pylily_factory',
                            self.home + '/NCREE_GIS',
                            6)  
            }

        if (self.hostname, self.platform ) in hostlist:
            arglist = hostlist[(self.hostname, self.platform )]
            self.factory    = arglist[0] if arg_factory     is None else arg_factory
            self.warehouse  = arglist[1] if arg_warehouse   is None else arg_warehouse
            self.cpu_code   = arglist[2]
     
        #check/create if not exists directory
        if  not os.path.exists(self.factory) :
            os.mkdir(self.factory)

        if  not os.path.exists(self.warehouse) :
            os.mkdir(self.warehouse)

    def set_syspath(self):
        import sys
        self.file = __file__
        sys.path.append(r'C:/Users/ctyang/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/pylily_pi')

    def check_module(self):
        for key, value in self.__dict__.items():
            if key != 'hostlist':
                print (key)
                print (value)
                print ('-------------------------------')

if __name__ == '__console__' or __name__ == '__main__':
    import sys, os
    #hostmetadata

    thishost = hostmetadata('m:/pylily_factory','g:/NCREE_GIS')
    thishost.check_module()
    



