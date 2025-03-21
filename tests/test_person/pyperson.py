
import abc


class Person(abc.ABC):
    def __init__(self, name, age,weight):
        self.name = name
        self.age = age
        self.weight = weight

    def grow(self):
        self.eat()
        self.drink()
        print(f'{self.name} grow ,now weight {self.weight}')
        self.do()

    def eat(self):
        self.weight +=1
        print('person eat')

    def drink(self):
        self.weight +=0.5

    @abc.abstractmethod
    def do(self):
        pass



class Adult(Person):
    def __init__(self, name, age,weight,company):
        super().__init__(name, age,weight)
        self.company = company

    def do(self):
        print(f'{self.name} work at {self.company}')

    def eat(self):
        super().eat()
        self.weight +=0.5
        print('adult eat')



class Child(Person):
    def __init__(self, name, age,weight,school):
        super().__init__(name, age,weight)
        self.school = school

    def do(self):
        print(f'{self.name} study at {self.school}')

    def eat(self):
        super().eat()
        self.weight +=0.2
        print('child eat')


if __name__ == '__main__':
    p1 = Adult('jack', 20, 100,'google')
    p2 = Child('tom', 10, 50,'primary')
    p1.grow()
    p2.grow()
