from tkinter import *
from tkinter import ttk
from tkinter import filedialog

root = Tk( )
root.withdraw()
class FolderOpener:
    def __init__(self):
        self.root = Tk()
        self.menu = Menu(root)
        self.root.config(menu=self.menu)
        file = Menu(self.menu)
        file.add_command(label = 'Open', command = self.OpenDir)
        file.add_command(label = 'Exit', command = lambda:exit())
        self.menu.add_cascade(label = 'File', menu = file)
        
        
        
    
        
#This is where we lauch the file manager bar.
    def OpenDir(self):
        self.root.withdraw()
        Title = root.title( "File Opener")
        label = ttk.Label(root, text ="Folder Opener",foreground="red",font=("Helvetica", 16))
        label.pack()
        folder_selected = filedialog.askdirectory()
        #Using try in case user types in unknown file or closes without choosing a file.
        try:
            return folder_selected
        except:
            print("No file exists")
        



#Menu Bar
if __name__ ==  "__main__":
    fileOpener = FolderOpener()
    folderName = fileOpener.OpenDir()
    print(folderName)
