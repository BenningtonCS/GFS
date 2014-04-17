#!/usr/bin/python

from Tkinter import Tk, Text, BOTH, W, N, E, S, Listbox, END, RIGHT, Y, Scrollbar
from ttk import Frame, Button, Label, Style
import socket, re, tkFileDialog
import functionLibrary as fL
from API import API


class GUI(Frame):
  
    def __init__(self, parent):
        Frame.__init__(self, parent)   

        self.api = API()

        self.SERVER_IP = '10.10.100.144'
        self.SERVER_PORT = 9666
        self.serverStatus = 0
        self.filesList = []


        self.parent = parent
        
        self.initUI()

        
    def initUI(self):
      
        self.parent.title("Bennington File System Client")
        self.style = Style()
        self.style.theme_use("default")
        self.pack(fill=BOTH, expand=1)


        self.file_opt = options = {}
        options['defaultextension'] = ''
        options['filetypes'] = ''
        options['initialdir'] = 'C:\\'
        options['initialfile'] = 'myfile.txt'
        options['parent'] = self
        options['title'] = 'This is a title'

        
        lbl = Label(self, text="Bennington File System Server Status:", foreground="black")
        lbl.grid(column=0, row=0, pady=4, padx=5)

        self.status = Label(self, text="", foreground="black")
        self.status.grid(column=3, row=0, pady=4, padx=5)


        #self.connectToServer()


        self.area = Listbox(self, height=20)
        self.area.grid(row=1, column=0, columnspan=1, rowspan=7, padx=5, sticky=N+W+E+S)

        
        self.getFiles()
        

        reconn = Button(self, text="Connect")
        reconn.grid(row=1, column=3)

        cbtn = Button(self, text="Disconnect")
        cbtn.grid(row=2, column=3, pady=4)

        abtn = Button(self, text="Upload", command=self.openFile)
        abtn.grid(row=3, column=3)

        dwnbtn = Button(self, text="Download", command=self.downloadFile)
        dwnbtn.grid(row=4, column=3)
        
        delbtn = Button(self, text="Delete", command=self.deleteFile)
        delbtn.grid(row=5, column=3)

        undelbtn = Button(self, text="Undelete")
        undelbtn.grid(row=6, column=3)

        refbtn = Button(self, text="Refresh List", command=self.getFiles)
        refbtn.grid(row=7, column=3)

        #hbtn = Button(self, text="Help")
        #hbtn.grid(row=5, column=0, padx=5)

        #obtn = Button(self, text="OK")
        #obtn.grid(row=5, column=3)



        quitButton = Button(self, text='Quit', command=self.exitProgram)
        quitButton.grid(sticky=W, padx=5, pady=4) 


    def downloadFile(self):
        fileName = self.currentSelection()
        self.api.read(fileName, 0, -1, fileName)


    def currentSelection(self):
        index = self.area.curselection()[0]
        fileName = self.area.get(index)
        return fileName


    def deleteFile(self):
        print "(:"


    def openFile(self):
        filepath = tkFileDialog.askopenfilename(**self.file_opt)
        filename = filepath.split('/')[-1]

        self.api.create(filename)
        self.api.append(filename, filepath, 1)

    def getFiles(self):
        try:
            data = self.api.fileList()
            
            data = re.split('\*', data)
            
            fileNames = []
            for item in data:
                tempitem = re.split('\^', item)
                for thing in tempitem:
                    if "|" in thing and not thing == "|":
                        fileNames.append(thing.strip('|'))


            for item in fileNames:
                if item not in self.filesList:
                    self.area.insert( END, item )
                    self.filesList.append(item)


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
            self.status['text'] = "Connected"
            self.status['foreground'] = "black"
        elif not self.serverStatus:
            self.status['text'] = "Failed to Connect"
            self.status['foreground'] = "orange"
        else:
            self.status['text'] = "Error"



    def disconnectFromServer(self):
        try:
            #self.s.close()
            print "disconnecting"
            self.status['text'] = "Disconnected"
            self.status['foreground'] = "red"

        except Exception as e:
            raise e

    def exitProgram(self):
        self.disconnectFromServer()
        self.quit()


              

def main():
  
    root = Tk()
    root.geometry("420x430+400+400")
    app = GUI(root)
    root.mainloop()  


if __name__ == '__main__':
    main()  


    