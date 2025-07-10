# ------------------ preprocess.py ------------------
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import WordPunctTokenizer

# Si es la primera vez que ejecutas, descomenta estas líneas:
#nltk.download('stopwords')

stemmer = PorterStemmer()
stop_words = set(stopwords.words('spanish'))  # o 'english' según tu idioma
tokenizer = WordPunctTokenizer()

def preprocess_text(text: str) -> list[str]:
    """
    Tokeniza con WordPunctTokenizer, filtra stopwords, elimina no alfabéticos
    y aplica stemming. Devuelve lista de tokens limpios.
    """
    # 1. Normalizar a minúsculas
    text = text.lower()
    # 2. Tokenizar sin sentence-split
    tokens = tokenizer.tokenize(text)
    # 3. Filtrar solo palabras y eliminar stopwords
    tokens = [t for t in tokens if t.isalpha() and t not in stop_words]
    # 4. Aplicar stemming
    tokens = [stemmer.stem(t) for t in tokens]
    return tokens