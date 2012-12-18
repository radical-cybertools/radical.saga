
class Base (Configurable) :

  def __init__ (self, adaptor_name, config_options={}) :
        self._adaptor_name = adaptor_name

        Configurable.__init__ (self, adaptor_name, config_options)


    def _get_name (self) :
        return self._adaptor_name

