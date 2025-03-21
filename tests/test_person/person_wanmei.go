package main888

import "fmt"

// Person 接口定义人的基本行为
type Person interface {
	Grow()
	Eat()
	Drink()
	Do()
}

// BasePerson 实现 Person 接口的基础结构体
type BasePerson struct {
	name   string
	age    int
	weight float64
	imp   Person
}

func (p *BasePerson) Grow() {
	p.imp.Eat()
	p.imp.Drink()
	fmt.Printf("%s grow, now weight %.1f\n", p.name, p.weight)
	p.imp.Do()
}

func (p *BasePerson) Eat() {
	fmt.Println("baseperson eat")
	p.weight += 1
}

func (p *BasePerson) Drink() {
	p.weight += 0.5
}

// func (p *BasePerson) Do() {
//     // 空实现或默认实现
// }

// Adult 结构体
type Adult struct {
	*BasePerson
	company string
}

func NewAdult(name string, age int, weight float64, company string) *Adult {
	this:= &Adult{
		BasePerson: &BasePerson{
			name:   name,
			age:    age,
			weight: weight,
		},
		company: company,
	}
	this.BasePerson.imp = this
	return this
}

func (a *Adult) Do() {
	fmt.Printf("%s work at %s\n", a.name, a.company)
}

func (a *Adult) Eat() {
	fmt.Printf("adult eat\n")
	a.BasePerson.Eat()
	a.weight += 0.88
	
}

// Child 结构体
type Child struct {
	*BasePerson
	school string
}

func NewChild(name string, age int, weight float64, school string) *Child {
	this:= &Child{
		BasePerson: &BasePerson{
			name:   name,
			age:    age,
			weight: weight,
		},
		school: school,
	}
	this.BasePerson.imp = this
	return this
}

func (c *Child) Do() {
	fmt.Printf("%s study at %s\n", c.name, c.school)
}

func (c *Child) Eat() {
	fmt.Print("child eat\n")
	c.BasePerson.Eat()
	c.weight += 0.22
	
}

func main() {
	p1 := NewAdult("jack", 20, 100, "google")
	p2 := NewChild("tom", 10, 50, "primary")

	p1.Grow()
	p2.Grow()

}