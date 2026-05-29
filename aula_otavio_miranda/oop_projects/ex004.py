class Anuncio:
    def __init__(self, titulo, preco, cidade):
        self.titulo = titulo
        self.preco = preco
        self.cidade = cidade

    def resumo(self):
        return f"{self.titulo} - R${self.preco} em {self.cidade}"
    
    def eh_barato(self, limite):
        return self.preco <= limite
    
anuncio = Anuncio("Carro Usado", 15000, "Manaus")
print(anuncio.resumo())
print(anuncio.eh_barato(20000))  # True
print(anuncio.eh_barato(10000))  # False