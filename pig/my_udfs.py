
def format_genere(word):
        if word =='Children\'s':
                return '1) Children\'s<yxg140730>'
        if word == 'Animation|Children\'s':
                return '1) Children\'s &  2) Animation<yxg140730>'
        if word == 'Children\'s|Adventure|Animation':
                return '1) Children\'s, 2) Adventure & 3) Animation <yxg140730>'
        return word