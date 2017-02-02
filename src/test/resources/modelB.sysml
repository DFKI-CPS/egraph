bdd [package] fsl::acs [ACS]
-----------------------------------------------------------

block Building
  references
    gate: Building[*] <- building
      derive: org_dom.dest
    building: Building[*] <- gate
    org_dom: Door[*] <- org
  constraints
    inv: not (gate->includes(self))

block Person
  operations
    -- P17: Any person is allowed to go through a door which communicates from the
    -- building he is to a building in which he is authorised to go. Moreover, this
    -- person must not be already engaged with another door.
    -- Defining (more or less) the admitted operation.
    -- A person p is admitted at a door q if:
    --   p is in a room where q originates from
    --   p is authorised to go to the room q leads to
    --   p is currently not in any door (dap)
    admitted(q: Door): Boolean { query }
      post P17: q.org = sit and
                aut->includes(q.dest) and
                dap_dom->isEmpty()
  references
    aut: Building[*]
    sit: Building[1] { subsets aut }
    -- P12: [...] Conversely, any person implied
    -- in the acceptance of a door cannot do the same of another.
    dap_dom: Door[0..1] <- dap
  constraints
    inv: aut->forAll(b|aut.building->includes(b))
    -- P13: For a door to be accepted for a certain person, this person should be
    -- inside the building of origin of that door. [...]
    inv: sit->includes(dap_dom.org)
    -- P13: [...] Moreover, this person should be authorised to enter the destination building of that same door.
    inv: aut->includes(dap_dom.dest)
    -- P12: [...] A door can
    -- only be accepted for one person at a time. Conversely, any person implied
    -- in the acceptance of a door cannot do the same of another.
    inv: Person.allInstances()->
             forAll(p1, p2 | p1.dap_dom->notEmpty()
                         and p1.dap_dom = p2.dap_dom
                     implies p1 = p2)

-- P11: The buildings communicate with each other via doors, which are
-- one-way. We there refer to buildings of origin and destination for each
-- door.
block Door
  values
    -- P14: The green light of a door is lit as long as the latter is accepted. [...]
    green: Boolean
      derive: dap->notEmpty()
    red: Boolean
  operations
    -- P13: For a door to be accepted for a certain person, this person should be
    -- inside the building of origin of that door. Moreover, this person should be
    -- authorised to enter the destination building of that same door.
    accept(p: Person)
      pre: not (green or red)
      pre: p.admitted(self)
      post: dap = p and p.dap_dom = self
    -- P14: [...] As soon as the person has got through, the door blocks itself again.
    -- In both cases, the green light goes off.
    pass_thru()
      pre: green
      post: dap@pre.sit = dest
        and dap@pre.dap_dom->isEmpty()
        and dap->isEmpty()
    -- P14: [...] After 30 seconds, if no-one goes through an accepted door, the latter will block
    -- automatically. In both cases, the green light goes off.
    off_grn()
      pre: green
      post: dap@pre.dap_dom->isEmpty()
        and dap->isEmpty()
  references
    org:  Building[1] <- org_dom
    dest: Building[1]
    -- P12: [...]A door can only be accepted for one person at a time.
    dap:  Person[0..1] <- dap_dom
  ports
    incoming: Person
    passing: Person
  owned behaviors
    state machine EnterBehavior
      initial state Waiting
        receive incoming(p) / accept(p) -> Accepting
      state Accepting
        after 30 seconds / off_grn() -> Waiting
        receive passing(p) / pass_thru() -> Waiting
