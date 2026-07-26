class Produto:
    def __init__(self, nome: str, preco: float, quantidade_estoque: int):
        self.nome = nome
        self.preco = preco
        self.quantidade_estoque = quantidade_estoque

    def exibir_informacoes(self):
        return f"Produto {self.nome} custa {self.preco} e temos {self.quantidade_estoque} em estoque"

    def calcular_valor_estoque(self):
        valor = self.preco * self.quantidade_estoque
        return f"O valor do estoque para o prduto {self.nome} custa R$ {valor}"

notebook = Produto("Notebook", 3500.00, 4)

print(notebook.exibir_informacoes())
print(notebook.calcular_valor_estoque())
