import tkinter as tk
from tkinter import messagebox
from tkinter.simpledialog import askstring
from hdfs import InsecureClient
from PIL import Image, ImageTk
import os

class HDFSFileManager1:
    def __init__(self, root):
        self.root = root
        self.root.title("HDFS File Manager")
        self.root.geometry("400x300")
        self.set_icon("file_manager.png")  # Set the icon for the window
        self.client = InsecureClient('http://localhost:9870', user="huser")
        
        self.file_list = tk.Listbox(root)
        self.file_list.pack(expand=True, fill="both")

        button_frame = tk.Frame(root)
        button_frame.pack()

        create_icon = Image.open("create_icon.png")
        create_icon.thumbnail((20, 20))
        create_icon = ImageTk.PhotoImage(create_icon)
        create_button = tk.Button(button_frame, text="Create", image=create_icon, compound="left", command=self.create_file)
        create_button.image = create_icon
        create_button.pack(side="left", padx=5)

        delete_icon = Image.open("delete_icon.png")
        delete_icon.thumbnail((20, 20))
        delete_icon = ImageTk.PhotoImage(delete_icon)
        delete_button = tk.Button(button_frame, text="Delete", image=delete_icon, compound="left", command=self.delete_file)
        delete_button.image = delete_icon
        delete_button.pack(side="left", padx=5)

        refresh_icon = Image.open("refresh_icon.png")
        refresh_icon.thumbnail((20, 20))
        refresh_icon = ImageTk.PhotoImage(refresh_icon)
        refresh_button = tk.Button(button_frame, text="Refresh", image=refresh_icon, compound="left", command=self.refresh_file_list)
        refresh_button.image = refresh_icon
        refresh_button.pack(side="left", padx=5)

    def set_icon(self, icon_path):
        if os.path.exists(icon_path):
            img = Image.open(icon_path)
            photo = ImageTk.PhotoImage(img)
            self.root.tk.call('wm', 'iconphoto', self.root._w, photo)

    def refresh_file_list(self):
        self.file_list.delete(0, tk.END)
        for file_name in self.client.list("/"):
            self.file_list.insert(tk.END, file_name)

    def create_file(self):
        file_name = askstring("Create File", "Enter the name of the file to create:")
        if file_name:
            try:
                with self.client.write("/" + file_name, encoding='utf-8') as writer:
                    writer.write("")  # Write an empty string to create the file
                self.refresh_file_list()
            except Exception as e:
                messagebox.showerror("Error", str(e))

    def delete_file(self):
        selected_file = self.file_list.curselection()
        if selected_file:
            file_name = self.file_list.get(selected_file)
            confirmed = messagebox.askokcancel("Delete File", f"Are you sure you want to delete {file_name}?")
            if confirmed:
                try:
                    self.client.delete("/" + file_name)
                    self.refresh_file_list()
                except Exception as e:
                    messagebox.showerror("Error", str(e))
        else:
            messagebox.showwarning("No File Selected", "Please select a file to delete.")

if __name__ == "__main__":
    root = tk.Tk()
    app = HDFSFileManager1(root)
    app.refresh_file_list()
    root.mainloop()

