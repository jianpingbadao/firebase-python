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


if __name__ == '__main__':
    myfb = MyFirebase(FIREBASE_URL)
