import json
import datetime
import os

from firebase import firebase

from firebase_profile import FIREBASE_URL


class MyFirebase:
    def __init__(self, url, auth=None):
        self.url = url
        self.fb = firebase.FirebaseApplication(url, auth)


    def rename_node(self, parent_url, old_node, new_node):
        """
        Rename the node at the given path

        Parameters
        ----------
        parent_url : str
            The url of the parent folder, which should end with '/'

        old_node : str
            The old name of the node

        new_node : str
            The new name of the node

        Returns
        -------
        rename successfully: bool
            True/False
        """
        try:
            content = self.fb.get(parent_url, old_node)
            if not content:
                print("Error: rename_node: old node %s does NOT exist yet" % (parent_url + old_node))
                return False

            content_new = self.fb.get(parent_url, new_node)
            if content_new:
                print("Error: rename_node: new node %s already exists" % (parent_url + new_node))
                return False

            self.fb.put(parent_url, new_node, content)
            self.fb.delete(parent_url, old_node)
            return True
        except:
            # TODO: revert partial changes if needed
            print("Error: rename_node: url %s, old name %s, new name %s" % (parent_url, old_node, new_node))
            return False


    def remove_node(self, parent_url, *nodes):
        """
        Remove the given nodes from the specified url

        Parameter
        ---------
        parent_url : str
            The url of the parent folder, which should end with '/'

        nodes : [str]
            The list of nodes to be removed
        """
        for node in nodes:
            # TODO: add some checking?
            self.fb.delete(parent_url, node)


    def save_database_to_file(self, output_file):
        """
        Save the database into file
        """
        print("Save database to file %s" % output_file)
        database = self.fb.get(self.url, None)
        # print(database)
        with open(output_file, 'w') as fp:
            json.dump(database, fp)


    def backup_database(self):
        """
        Backup the database
        """
        current_time = datetime.datetime.utcnow()
        backup_folder = self.get_backup_folder()

        backup_file = os.path.join(backup_folder, 'UTC - ' + str(current_time) + '.json')
        self.save_database_to_file(backup_file)


    def get_latest_backup(self):
        """
        Get the latest backup file
        """
        backup_folder = self.get_backup_folder()
        files = os.listdir(backup_folder)
        files = [f for f in files if os.path.isfile(os.path.join(backup_folder, f)) and not f.startswith('.')]
        files = sorted(files)
        if files:
            latest = files[-1]
            return os.path.join(backup_folder, latest)

        return None


    def recover_from_latest_backup(self):
        """
        Load the latest backup into the database
        """
        latest_backup = self.get_latest_backup()
        if not latest_backup:
            print("No backup available.")
            return

        with open(latest_backup, 'r') as fp:
            database = json.load(fp)

        # print(database)
        print("recover from %s" % latest_backup)
        for top_key, values in database.items():
            self.fb.put(self.url, top_key, values)


    def get_backup_folder(self):
        """
        Get the backup folder
        """
        backup_folder = os.path.join(os.getcwd(), 'backup')
        if not os.path.isdir(backup_folder):
            os.mkdir(backup_folder)
        return backup_folder



if __name__ == '__main__':
    myfb = MyFirebase(FIREBASE_URL)
