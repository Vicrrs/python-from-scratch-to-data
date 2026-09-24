# Encapsulamento com atributo privado e property

class Funcionario:
    def __init__(self, nome: str, salario: float):
        self.nome = nome
        self.salario = salario

    @property
    def salario(self) -> float:
        return self.__salario

    @salario.setter
    def salario(self, novo_salario: float) -> None:
        if novo_salario < 0:
            raise ValueError("O salario nao pode ser negativo.")

        self.__salario = novo_salario

    def aplicar_aumento(self, percentual: float) -> None:
        if percentual <= 0:
            raise ValueError("O percentual deve ser maior que zero.")

        aumento = self.__salario * percentual / 100
        self.salario = self.__salario + aumento

funcionario = Funcionario("Victor", 5000)

funcionario.aplicar_aumento(20)
print(funcionario.salario)
