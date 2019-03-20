import json
import datetime
import os

from firebase import firebase

from firebase_profile import FIREBASE_URL


class Pothole:
    def __init__(self, lat: float, lng: float, depth: str=None, length: str=None, image: str=None):
        self.lat = lat
        self.lng = lng
        self.depth = depth
        self.length = length
        self.image = image


    def to_dict(self):
        """
        Format the pothole to a dictionary so that it is easier to be added into the database.
        """
        obj = {}
        obj['latitude'] = self.lat
        obj['longitude'] = self.lng
        if self.depth:
            obj['depth'] = self.depth
        if self.length:
            obj['length'] = self.length
        if self.image:
            obj['image'] = self.image
        return obj



class MyFirebase:
    def __init__(self, url, auth=None):
        self.url = url
        self.fb = firebase.FirebaseApplication(url, auth)
        self.url_potholes = self.url + '/potholes'


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


    def pothole_exist(self, pothole: Pothole):
        """
        Check if the given pothole is already existing in the database.

        Returns
        -------
        True if pothole already exists.
        """
        potholes = self.fb.get(self.url_potholes, None)
        for _, pot in potholes.items():
            if pot['latitude'] == pothole.lat and pot['longitude'] == pothole.lng:
                return True
        return False


    def post_pothole(self, pothole: Pothole):
        """
        Add given pothole into the database via POST
        """
        if self.pothole_exist(pothole):
            print("pothole exists")
            return

        self.fb.post(self.url_potholes, pothole.to_dict())



def init_db(mfb: MyFirebase):
    """
    Add a couple of potholes into the database
    """
    # mfb.backup_database()

    potholes = [(42.999938,-78.797406), (42.980365,-78.807876), (42.999499,-78.794131)]
    for pot in potholes:
        pothole = Pothole(pot[0], pot[1], '40cm', '3cm', '<img src="data:image/png;base64,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" alt="" />')
        mfb.post_pothole(pothole)



if __name__ == '__main__':
    myfb = MyFirebase(FIREBASE_URL)
    init_db(myfb)
