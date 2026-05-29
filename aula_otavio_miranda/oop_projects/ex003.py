class Produto:
    def __init__(self, nome, preco, categoria):
        self.nome = nome
        self.preco = preco
        self.categoria = categoria

    def descricao(self):
        return f"{self.nome} custa R$ {self.preco} na categoria de {self.categoria}"
    
    def aplicar_desconto(self, percentual):
        desconto = self.preco * (percentual / 100)
        self.preco -= desconto
        return self.preco
    
produto = Produto("Notebook", 3500, "Eletrônicos")
produto.aplicar_desconto(10)

print(produto.descricao())
# Notebook custa R$ 3150.0 na categoria Eletrônicos