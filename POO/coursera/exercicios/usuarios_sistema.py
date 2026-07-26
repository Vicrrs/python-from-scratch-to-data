# 3. Atributo de classe e atributo de instância

class Usuario:
    total_usuarios = 0

    def __init__(self, nome:str, email: str):
        self.nome = nome
        self.email = email

        Usuario.total_usuarios += 1

    def exibir_usuario(self) -> str:
        return f"{self.nome} - {self.email}"

    @classmethod
    def exibir_total_usuarios(cls) -> str:
        return f"Total de usuários: {cls.total_usuarios}"



usuario1 = Usuario("Victor", "victor@email.com")
usuario2 = Usuario("Ana", "ana@email.com")
usuario3 = Usuario("Carlos", "carlos@email.com")

print(usuario1.exibir_usuario())
print(Usuario.exibir_total_usuarios())