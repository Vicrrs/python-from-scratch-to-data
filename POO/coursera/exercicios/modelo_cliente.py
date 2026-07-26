# Classe, objeto e atributos

class Cliente: # classe
    def __init__(self, nome: str, idade: int): # metodos
        self.nome = nome    # atributos
        self.idade = idade  # atributos

    def apresentar(self) -> str:
        return f"Olá, meu nome é {self.nome} e tenho {self.idade} anos."

cliente1 = Cliente("Victor", 30)
cliente2 = Cliente("Jack", 26)

print(cliente1.apresentar())
print(cliente2.apresentar())
