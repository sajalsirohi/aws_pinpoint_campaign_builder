import abc

class Channel(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def update_channel(self):
        raise NotImplementedError('Must define enable_channel method')


    @abc.abstractmethod
    def delete_channel(self):
        raise NotImplementedError('Must define delete_channel method')


    @abc.abstractmethod
    def channel_details(self):
        raise NotImplementedError('Must define channel_details method')


    @abc.abstractmethod
    def create_template(self):
        raise NotImplementedError('Must define create_template method')


    @abc.abstractmethod
    def list_template_versions(self):
        raise NotImplementedError('Must define list_template_versions method')


    @abc.abstractmethod
    def set_custom_message(self):
        raise NotImplementedError('Must define set_custom_message method')
