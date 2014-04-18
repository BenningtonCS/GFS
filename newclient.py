#!/usr/bin/python

from Tkinter import Tk, Text, BOTH, W, N, E, S, Listbox, END, RIGHT, Y, Scrollbar
from ttk import Frame, Button, Label, Style
import socket, re, tkFileDialog, config
import functionLibrary as fL
from API import API


# GUI Object definition
class GUI(Frame):
    # In intialize the object, taking in the parent as in input parameter
    def __init__(self, parent):
        Frame.__init__(self, parent)   
        # Initialize the api
        self.api = API()
        # Set the ip and port to communicate with the master server
        self.SERVER_IP = config.masterip
        self.SERVER_PORT = config.port
        # Set the initial server status to 0, will change to 1 if server is active
        #self.serverStatus = 0
        # Declare a list which will hold the files that have been flagged for deletion
        self.toDelete = []

        self.parent = parent
        # Initialize the GUI
        self.initUI()

    # Function to initialize UI
    def initUI(self):
        # Set the name of the UI window
        self.parent.title("Bennington File System Client")
        # Set the style using the default theme
        self.style = Style()
        self.style.theme_use("default")
        self.pack(fill=BOTH, expand=1)

        # Set the "Open File" options
        self.file_opt = options = {}
        # Allow for any file to be choosable
        options['defaultextension'] = ''
        options['filetypes'] = ''
        # Set the directory window will open up to initially
        options['initialdir'] = 'C:\\'
        options['parent'] = self

        # Create a label object which holds the text labeling the listbox
        lbl = Label(self, text="Bennington File System Files List", foreground="black")
        # Place the text in the top left
        lbl.grid(column=0, row=0, pady=4, padx=5)

        # Create the listbox, which will contain a list of all the files on the system
        self.area = Listbox(self, height=20)
        # Place the lisbox in the UI frame
        self.area.grid(row=1, column=0, columnspan=1, rowspan=10, padx=5, sticky=N+W+E+S)

        # Ask the master server which files it has, then populate the listbox with the response
        self.getFiles()
        
        # Create a button labeled 'Upload', and bind the uploadFile() function to it
        uploadbtn = Button(self, text="Upload", command=self.uploadFile)
        # Place the button in the UI frame
        uploadbtn.grid(row=1, column=3)

        # Create a button labeled 'Download', and bind the downloadFile() function to it
        dwnbtn = Button(self, text="Download", command=self.downloadFile)
        # Place the button in the UI frame
        dwnbtn.grid(row=2, column=3)
        
        # Create a button labeled 'Delete', and bind the deleteFile() function to it
        delbtn = Button(self, text="Delete", command=self.deleteFile)
        # Place the button in the UI frame
        delbtn.grid(row=3, column=3)

        # Create a button labeled 'Undelete', and bind the undeleteFile() function to it
        undelbtn = Button(self, text="Undelete", command=self.undeleteFile)
        # Place the button in the UI frame
        undelbtn.grid(row=4, column=3)

        # Create a button labeled 'Refresh List', and bind the getFiles() function to it
        refbtn = Button(self, text="Refresh List", command=self.getFiles)
        # Place the button in the UI frame
        refbtn.grid(row=5, column=3)

        # Create a button labeled 'Quit', and bind the exitProgram() function to it
        quitButton = Button(self, text='Quit', command=self.exitProgram)
        # Place the button in the UI frame
        quitButton.grid(sticky=W, padx=5, pady=4) 


    # Downloads the active selection in the listbox
    def downloadFile(self):
        # Get the filename from the active listbox item
        fileName = self.currentSelectionFileName()
        # Call the API read function to get all the data associated with that file
        self.api.read(fileName, 0, -1, fileName)


    # Get the file name of the active selection in the listbox
    def currentSelectionFileName(self):
        # Get the index of the active selection
        index = currentSelectionIndex()
        # From the listbox object, pass in the index to get the filename
        fileName = self.area.get(index)
        return fileName


    # Get the index of the active selection in the listbox
    def currentSelectionIndex(self):
        # Get the index of the active selection
        index = self.area.curselection()[0]
        return index


    # Mark the active selection in the listbox for deletion
    def deleteFile(self):
        # Get the index of the current active selection
        index = self.currentSelectionIndex()
        # Get the filename of the current active selection
        filename = self.area.get(index)
        # Call the API function to mark file for deletion
        self.api.delete(filename)
        # Change the background color of the selection to denote it has been marked for deletion
        self.area.itemconfig(index, {'bg':'salmon'}) 
        # Append the filename to the toDelete list
        self.toDelete.append(filename)


    # Unmarks the active selection in the listbox for deletion
    def undeleteFile(self):
        # Get the index of the current active selection
        index = self.currentSelectionIndex()
        # Get the filename of the current active selection
        filename = self.area.get(index)
        # See if the file has been marked for deletion
        if filename in self.toDelete:
            # Call the API function to unmark file for deletion
            self.api.undelete(filename)
            # Change the background color of the selection to denote it is no longer marked for deletion
            self.area.itemconfig(index, {'bg':'white'})
            # Remove the filename from the toDelete list
            self.toDelete.remove(filename)


    # Upload a file from local machine to the distributed file system
    def uploadFile(self):
        # Get the file name and file path of the file the user wants to upload
        fileinfo = self.openFile()
        # Create a file with the filename provided
        self.api.create(fileinfo[0])
        # Append the file data from the file at the given file path
        self.api.append(fileinfo[0], fileinfo[1], 1)
        # Once that is complete, refresh the file list in the listbox
        self.getFiles()


    # Prompt the user to select a file
    def openFile(self):
        # Prompt the user to select a file, and store the returned file path
        filepath = tkFileDialog.askopenfilename(**self.file_opt)
        # Parse the filepath, and store the last element of the split as the file name
        filename = filepath.split('/')[-1]
        # Return a list containing the file name and file path
        return [filename, filepath]


    # Get the list of existing files from the master server
    def getFiles(self):
        try:
            # Get the file data using the API's fileList() function
            # At this stage, the data has lots of extra things in it, so it needs to be cleaned up
            data = self.api.fileList()
            # Split the string at every * character
            data = re.split('\*', data)
            # Declare an empty array which will hold the file names found from the parsing
            fileNames = []
            for item in data:
                # Split the element at every ^ character
                tempitem = re.split('\^', item)
                for thing in tempitem:
                    # If the element has a | in it, but is not just a |, then it is the filename
                    if "|" in thing and not thing == "|":
                        # Append the filename (with the | stripped out) to the fileNames list
                        fileNames.append(thing.strip('|'))

            # If the file is marked for deletion and still in the fileNames list
            # keep the file in the toDelete list
            temp = []
            for item in self.toDelete:
                if item in fileNames:
                    temp.append(item)

            # Update the toDelete list
            self.toDelete = temp

            # Remove all entries in the listbox
            self.area.delete(0, END)

            # Populate the listbox with the file names returned from the master
            for item in fileNames:
                self.area.insert( END, item )
                self.checkIfMarked(item)

        except Exception as e:
            raise e


    # Check to see if the provided file name exists in the toDelete list
    def checkIfMarked(self, fileName):
        # If the file name is in the toDelete list
        if fileName in self.toDelete:
            # Change the background color of the element to denote that it has been marked for deletion
            self.area.itemconfig(END, {'bg':'salmon'})


    # An exit function that quits the UI (any additional clean up pre-close should go in here)
    def exitProgram(self):
        self.quit()


    


              

def main():
  
    root = Tk()
    root.geometry("420x430+400+400")
    app = GUI(root)
    root.mainloop()  


if __name__ == '__main__':
    main()  


    