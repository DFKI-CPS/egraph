bdd [package] acs [ACS]
-----------------------------------------------------------

block Building
	references
		gate: Building[*] <- building
			derive: org_dom.dest
		building: Building[*] <- gate
		org_dom: Door[*] <- org
	constraints
		inv: not gate->includes(self)

block Person
	operations
		admitted(q: Door): Boolean
			post P17: q.org = self.sit and
								self.aut->includes(q.dest) and
								self.dap_dom->isEmpty()
	references
		aut: Building[*]
		sit: Building[1] { subsets aut }
		dap_dom: Door[0..1] <- dap
	constraints
		inv: aut->forAll(b|self.aut.building->includes(b))
		inv: aut->includes(dap_dom.dest)
		inv: sit->includes(dap_dom.org)
		inv: Person.allInstances()->
			forAll(p1, p2 |
				p1.dap_dom->notEmpty() and p1.dap_dom = p2.dap_dom implies p1 = p2)

block Door
	values
		green: Boolean
			derive: dap->notEmpty()
		red: Boolean
	operations
		accept()
			pre: not (green or red)
			pre: dap.admitted(self)
			post: dap.dap_dom = self
		refuse()
			pre: not (green or red)
			pre: not dap.admitted(self)
			post: red
		pass_thru()
			pre: green
			post: dap@pre.sit = dest
				and dap@pre.dap_dom->isEmpty
				and dap->isEmpty()
		off_grn()
			pre: green
			post: dap@pre.dap_dom->isEmpty()
				and dap->isEmpty()
		off_red()
			post: not red
	references
		org:  Building[1] <- org_dom
		dest: Building[1]
		dap:  Person[0..1] <- dap_dom
	ports
		incoming: Person[2]
		passing: Person[2]
	owned behaviors
		state machine DoorBehavior
			initial state Waiting
				receive incoming(p) / accept(p) -> Accepting
				receive incoming(p) / refuse(p) -> Refusing
			state Accepting
				after 30 seconds / off_grn() -> Waiting
				receive passing(p) / pass_thru() -> Waiting
			state Refusing
				after 30 seconds / off_red() -> Waiting
