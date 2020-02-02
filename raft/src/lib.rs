#![feature(integer_atomics)]
#![deny(clippy::all)]
#![allow(clippy::single_match)]
#![allow(clippy::while_let_loop)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

#[allow(dead_code)]
/// A place holder for suppressing unused_variables warning.
fn your_code_here<T>(_: T) -> ! {
    unimplemented!()
}
