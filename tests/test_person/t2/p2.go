package main

import "fmt"

// Person 接口定义了所有人类共有的行为
type Person interface {
	Grow()
	Eat()
	Drink()
	Do()
	GetName() string
	GetWeight() float64
	SetWeight(weight float64)
}

// BasePerson 实现了Person接口的基本功能
type BasePerson struct {
	name   string
	age    int
	weight float64
}

func (p *BasePerson) Grow() {
	p.Eat()
	p.Drink()
	fmt.Printf("%s grow, now weight %.1f\n", p.name, p.weight)
	p.Do()
}

func (p *BasePerson) Eat() {
	fmt.Println("BasePerson.Eat() 被调用")
	p.weight += 1
}

func (p *BasePerson) Drink() {
	p.weight += 0.5
}

func (p *BasePerson) GetName() string {
	return p.name
}

func (p *BasePerson) GetWeight() float64 {
	return p.weight
}

func (p *BasePerson) SetWeight(weight float64) {
	p.weight = weight
}

// Do 是一个空实现，将被子类覆盖
func (p *BasePerson) Do() {
	// 这是一个抽象方法，需要被子类实现
}

// Adult 表示成年人
type Adult struct {
	BasePerson
	company string
}

// 创建新的Adult实例
func NewAdult(name string, age int, weight float64, company string) *Adult {
	return &Adult{
		BasePerson: BasePerson{
			name:   name,
			age:    age,
			weight: weight,
		},
		company: company,
	}
}

func (a *Adult) Do() {
	fmt.Printf("%s work at %s\n", a.name, a.company)
}

func (a *Adult) Eat() {
	fmt.Println("Adult.Eat() 被调用")
	a.BasePerson.Eat()
	a.weight += 0.88
}

// Child 表示儿童
type Child struct {
	BasePerson
	school string
}

// 创建新的Child实例
func NewChild(name string, age int, weight float64, school string) *Child {
	return &Child{
		BasePerson: BasePerson{
			name:   name,
			age:    age,
			weight: weight,
		},
		school: school,
	}
}

func (c *Child) Do() {
	fmt.Printf("%s study at %s\n", c.name, c.school)
}

func (c *Child) Eat() {
	fmt.Println("Child.Eat() 被调用")
	c.BasePerson.Eat()
	c.weight += 0.22
}

func main() {
	p1 := NewAdult("jack", 20, 100, "google")
	p2 := NewChild("tom", 10, 50, "primary")
	p1.Grow()
	p2.Grow()
}