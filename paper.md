---
title: 'Application Skeleton: Generating Synthetic Applications for Infrastructure Research'
tags:
  - computational science
  - data science
  - application modeling
  - system modeling
  - performance modeling
  - parallel and distributed systems
authors:
 - name: Zhao Zhang
   orcid: 0000-0001-5921-0035
   affiliation: AMPLab and BIDS, University of California, Berkeley
 - name: Daniel S. Katz
   orcid: 0000-0001-5934-7525
   affiliation: National Center for Supercomputing Applications, University of Illinois Urbana-Champaign
 - name: Andre Merzky
   orcid: 0000-0002-7228-4327
   affiliation: RADICAL Laboratory, Rutgers University
 - name: Matteo Turilli
   orcid: 0000-0003-0527-1435
   affiliation: RADICAL Laboratory, Rutgers University
 - name: Shantenu Jha
   orcid: 0000-0002-5040-026X
   affiliation: RADICAL Laboratory, Rutgers University
 - name: Yadu Nand
   orcid: 0000-0002-9162-6003
   affiliation: Computation Institute, University of Chicago
date: 5 May 2016
bibliography: paper.bib
---

# Summary
Application Skeleton is a simple and powerful tool to build simplified synthetic science and engineering applications (for example, modeling and simulation, data analysis) with runtime and I/O close to that of the real applications. It is intended for applied computer scientists who need to use science and engineering applications to verify the effectiveness of new systems designed to efficiently run such applications, so that they can bypass obstacles that they often encounter when accessing and building real science and engineering applications. Using the applications generated by Application Skeleton guarantees that the CS systems' effectiveness on synthetic applications will apply to the real applications.

Application Skeleton can generate bag-of-task, (iterative) map-reduce, and (iterative) multistage workflow applications. These applications are represented as a set of tasks, a set of input files, and a set of dependencies.  These applications can be generally considered many-task applications, and once created, can be run on single-core, single-node, multi-core, or multi-node (distributed or parallel) computers, depending on what workflow system is used to run them.  The generated applications are compatible with workflow system such as Swift [@SWIFT07, @SWIFT09, @SWIFT11] and Pegasus [@PEGASUS04, @PEGASUS05], as well as the ubiquitous UNIX shell.
The application can also be created as a generic JSON object that can be used by other systems such as the AIMES [@AIMES15] middleware.

# References
