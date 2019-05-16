class SingletonInstance(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(SingletonInstance, cls).__new__(cls)
        else:
            def init_pass(self, *args, **kwargs):
                pass
            cls.__init__ = init_pass

        return cls._inst
