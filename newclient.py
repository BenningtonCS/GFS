#!/usr/bin/python

from Tkinter import Tk, Text, BOTH, W, N, E, S, Listbox, END, RIGHT, Y, Scrollbar
from ttk import Frame, Button, Label, Style
import socket, re
import functionLibrary as fL


class GUI(Frame):
  
    def __init__(self, parent):
        Frame.__init__(self, parent)   

        self.SERVER_IP = '10.10.100.144'
        self.SERVER_PORT = 9666
        self.serverStatus = 0
         
        self.parent = parent
        
        self.initUI()

        
    def initUI(self):
      
        self.parent.title("Bennington File System Client")
        self.style = Style()
        self.style.theme_use("default")
        self.pack(fill=BOTH, expand=1)

        self.columnconfigure(1, weight=1)
        self.columnconfigure(3, pad=7)
        self.rowconfigure(3, weight=1)
        self.rowconfigure(5, pad=7)
        
        self.lbl = Label(self, text="Bennington File System Server Status:")
        self.lbl.grid(sticky=W, pady=4, padx=5)


        self.connectToServer()


        self.area = Listbox(self)
        self.area.grid(row=1, column=0, columnspan=2, rowspan=4, padx=5, sticky=E+W+S+N)

        
        if self.serverStatus == 1:
            self.getFiles()
        

        reconn = Button(self, text="Connect", command=self.connectToServer)
        reconn.grid(row=1, column=3)

        cbtn = Button(self, text="Disconnect", command=self.disconnectFromServer)
        cbtn.grid(row=2, column=3, pady=4)

        #abtn = Button(self, text="Activate")
        #abtn.grid(row=3, column=3)
        
        #hbtn = Button(self, text="Help")
        #hbtn.grid(row=5, column=0, padx=5)

        #obtn = Button(self, text="OK")
        #obtn.grid(row=5, column=3)

        quitButton = Button(self, text='Quit', command=self.exitProgram)
        quitButton.grid(row=5, column=3) 


    def getFiles(self):
        try:
            fL.send(self.s, "FILELIST|x")
            data = fL.recv(self.s)
            
            data = re.split('\*', data)
            
            fileNames = []
            for item in data:
                tempitem = re.split('\^', item)
                for thing in tempitem:
                    if "|" in thing:
                        fileNames.append(thing.strip('|'))


            for item in fileNames:
                self.area.insert( END, item )


        except Exception as e:
            raise e


    def connectToServer(self):

        try:
            # Create a TCP socket instance
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set the timeout of the socket
            self.s.settimeout(3)
            # Allow the socket to re-use address
            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Connect to the chunkserver over the specified port
            self.s.connect((self.SERVER_IP, self.SERVER_PORT))
            # If the connection was successful, return a success value
            self.serverStatus = 1

            print "connected"
            
        except (socket.error, socket.timeout) as e:
            # If could not connect to the server, return fail value
            self.serverStatus = 0
            print "couldnt connect"

        if self.serverStatus:
            self.lbl['text'] = "Bennington File System Sever Status: Connected"
        elif not self.serverStatus:
            self.lbl['text'] = "Bennington File System Sever Status: Failed to Connect"
        else:
            self.lbl['text'] = "Error occured when attempting to connect to server"



    def disconnectFromServer(self):
        try:
            self.s.close()
            print "disconnecting"
            self.lbl['text'] = "Bennington File System Sever Status: Disconnected"

        except Exception as e:
            raise e

    def exitProgram(self):
        self.disconnectFromServer()
        self.quit()


              

def main():
  
    root = Tk()
    root.geometry("550x400+400+400")
    app = GUI(root)
    root.mainloop()  


if __name__ == '__main__':
    main()  


    