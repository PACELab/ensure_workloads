#! /usr/bin/python3

import sys,math,json,subprocess
from datetime import time
import time
import boto
import boto.s3.connection
import boto3

access_key = "TBD"
secret_key = "TBD" #os.getenv('AWS_SECRET_ID',"")
hostname = "TBD" 
portNum = "TBD"

constVars = {
    'block_length' : 8,
    'GAMMA' : 1.4,
    'iterations' :  2000,
    'NDIM' :  3,
    'NNB' :  4,
    'RK' :  3, 
    'ff_mach' :  1.2,
    'deg_angle_of_attack' :  0.0,
    'VAR_DENSITY' : 0,
    'VAR_MOMENTUM' : 1
}

constVars['VAR_DENSITY_ENERGY'] = constVars['VAR_MOMENTUM'] + constVars['NDIM']
constVars['NVAR'] = constVars['VAR_DENSITY_ENERGY']+1
constVars['angle_of_attack'] = float(3.1415926535897931 / 180.0) * float(constVars['deg_angle_of_attack']);
constVars['numParts'] = 1


def compute_flux_contribution(density,momentum,density_energy,pressure,velocity,fc_momentum_x,fc_momentum_y,fc_momentum_z,fc_density_energy):
    #(float& density, float3& momentum, float& density_energy, float& pressure, float3& velocity, float3& fc_momentum_x, float3& fc_momentum_y, float3& fc_momentum_z,float3& fc_density_energy)
    
    fc_momentum_x['x'] = velocity['x']*momentum['x'] + pressure;
    fc_momentum_x['y'] = velocity['x']*momentum['y'];
    fc_momentum_x['z'] = velocity['x']*momentum['z'];

    fc_momentum_y['x'] = fc_momentum_x['y'];
    fc_momentum_y['y'] = velocity['y']*momentum['y'] + pressure;
    fc_momentum_y['z'] = velocity['y']*momentum['z'];

    fc_momentum_z['x'] = fc_momentum_x['z'];
    fc_momentum_z['y'] = fc_momentum_y['z'];
    fc_momentum_z['z'] = velocity['z']*momentum['z'] + pressure;

    de_p = density_energy+pressure;
    fc_density_energy['x'] = velocity['x']*de_p;
    fc_density_energy['y'] = velocity['y']*de_p;
    fc_density_energy['z'] = velocity['z']*de_p;


def initParams(allParams):
    ff_variable = [ 0.0 for x in range(constVars['NVAR'])]
    ff_variable[constVars['VAR_DENSITY']] = float(1.4);
    
    ff_pressure = float(1.0);
    ff_speed_of_sound = math.sqrt(constVars['GAMMA']*ff_pressure / ff_variable[constVars['VAR_DENSITY']]);
    ff_speed = float(constVars['ff_mach'])*ff_speed_of_sound;   

    ff_velocity = {}; ff_momentum = {}; 
    ff_flux_contribution_momentum_x = {}; ff_flux_contribution_momentum_y = {}; ff_flux_contribution_momentum_z = {}; ff_flux_contribution_density_energy = {}
    ff_velocity['x'] = ff_speed*float(math.cos(float(constVars['angle_of_attack'])));
    ff_velocity['y'] = ff_speed*float(math.sin(float(constVars['angle_of_attack'])));
    ff_velocity['z'] = 0.0;

    ff_variable[constVars['VAR_MOMENTUM']+0] = ff_variable[constVars['VAR_MOMENTUM']] * ff_velocity['x'];
    ff_variable[constVars['VAR_MOMENTUM']+1] = ff_variable[constVars['VAR_MOMENTUM']] * ff_velocity['y'];
    ff_variable[constVars['VAR_MOMENTUM']+2] = ff_variable[constVars['VAR_MOMENTUM']] * ff_velocity['z'];

    ff_variable[constVars['VAR_DENSITY_ENERGY']] = ff_variable[constVars['VAR_MOMENTUM']]*(float(0.5)*(ff_speed*ff_speed)) + (ff_pressure / float(constVars['GAMMA']-1.0));
    ff_momentum['x'] = ff_variable[constVars['VAR_MOMENTUM']+0]
    ff_momentum['y'] = ff_variable[constVars['VAR_MOMENTUM']+1]
    ff_momentum['z'] = ff_variable[constVars['VAR_MOMENTUM']+2]

    compute_flux_contribution(ff_variable[constVars['VAR_MOMENTUM']], ff_momentum, ff_variable[constVars['VAR_DENSITY_ENERGY']], ff_pressure, ff_velocity, ff_flux_contribution_momentum_x, 
        ff_flux_contribution_momentum_y, ff_flux_contribution_momentum_z, ff_flux_contribution_density_energy);
    allParams['ff_variable'] = ff_variable
    allParams['ff_momentum'] = ff_momentum
    allParams['ff_flux_contribution_momentum_x'] = ff_flux_contribution_momentum_x
    allParams['ff_flux_contribution_momentum_y'] = ff_flux_contribution_momentum_y
    allParams['ff_flux_contribution_momentum_z'] = ff_flux_contribution_momentum_z
    allParams['ff_flux_contribution_density_energy'] = ff_flux_contribution_density_energy
    
    #return allParams

def readFile(path,allParams):

    readFile = open(path,'r').readlines()
    fileLineIdx = 0
    nel = int(readFile[fileLineIdx].strip()); fileLineIdx+=1;
    nelr = int(constVars['block_length']*((nel / constVars['block_length'] )+min(1, nel % constVars['block_length'])));

    areas = [0.0 for x in range(nelr)]
    elements_surrounding_elements = [0 for x in range(nelr*constVars['NNB'])];
    normals = [0.0 for x in range(constVars['NDIM']*constVars['NNB']*nelr)];

    allParams['nel'] = nel
    allParams['areas'] = areas
    allParams['elements_surrounding_elements'] = elements_surrounding_elements
    allParams['normals'] = normals
    allParams['nelr'] = nelr

    print ("\t nel: %s nelr: %s "%(nel,nelr))
    for i in range(nel):
        nextLine = readFile[fileLineIdx].split("\t"); fileLineIdx+=1; curLineElements = []
        for curEle in nextLine:
            temp = curEle.split(" ")
            for curTempEle in temp:
                if(curTempEle!=""):
                    curTempEle = curTempEle.split("\n")
                    if(curTempEle[0]==""): continue;
                    curLineElements.append(float(curTempEle[0]))

        curLineEleIdx = 0; 
        areas[i] = curLineElements[curLineEleIdx]; curLineEleIdx+=1; 
        for j in range(constVars['NNB']):
            elements_surrounding_elements[i + j*nelr] = curLineElements[curLineEleIdx]; curLineEleIdx+=1;
            if(elements_surrounding_elements[i+j*nelr] < 0): elements_surrounding_elements[i+j*nelr] = -1;
            elements_surrounding_elements[i + j*nelr]-=1; #it's coming in with Fortran numbering

            for k in range(constVars['NDIM']):
                normals[i + (j + k*constVars['NNB'])*nelr] = curLineElements[curLineEleIdx]; curLineEleIdx+=1;
                normals[i + (j + k*constVars['NNB'])*nelr] = (-1*normals[i + (j + k*constVars['NNB'])*nelr]);

    last = nel-1
    for i in range(nel,nelr):
        areas[i] = areas[last]
        for j in range(constVars['NNB']):
            elements_surrounding_elements[i + j*nelr] = elements_surrounding_elements[last + j*nelr];
            for k in range(constVars['NDIM']): 
                normals[i + (j + k*constVars['NNB'])*nelr] = normals[last + (j + k*constVars['NNB'])*nelr];

    return allParams


def readFluxesFile(allParams,readFilename,startIdx):
    readFile = open(readFilename,'r').readlines()
    curIdx = startIdx
    """
    fluxes[i + constVars['VAR_DENSITY']*nelr] = flux_i_density;
    fluxes[i + (constVars['VAR_MOMENTUM']+0)*nelr] = flux_i_momentum['x'];
    fluxes[i + (constVars['VAR_MOMENTUM']+1)*nelr] = flux_i_momentum['y'];
    fluxes[i + (constVars['VAR_MOMENTUM']+2)*nelr] = flux_i_momentum['z'];
    fluxes[i + constVars['VAR_DENSITY_ENERGY']*nelr] = flux_i_density_energy;
    # Since fluxes of a type (density, mom_x, mom_y,..) are contiguous
    """
    numLines = len(readFile)
    offset = [ constVars['VAR_DENSITY'],constVars['VAR_MOMENTUM'],constVars['VAR_MOMENTUM']+1,constVars['VAR_MOMENTUM']+2,constVars['VAR_DENSITY'] ] 
    if(numLines!= allParams['nelr']*constVars['NVAR']):
        print ("\t readFilename: %s numLines: %d allParams['nelr']: %d constVars['NVAR']: %d "%(readFilename,numLines,allParams['nelr'],constVars['NVAR']))
    lineNum = 0;
    for curVar in range(constVars['NVAR']):
        prod = allParams['nelr']*constVars['numParts']
        prodOffset = prod * offset[curVar];
        for curIdx in range(startIdx+prodOffset,startIdx+prodOffset+allParams['nel']):
            curLine = readFile[lineNum]; lineNum+=1;
            allParams['sync_fluxes'][curIdx] = float(curLine.split("\n")[0].strip());
    
    return

def initialize_variables(nelr,variables,ff_variable):
    print ("\t In initialize_variables")
    #(int nelr, float* variables, float* ff_variable)
    #pragma omp parallel for default(shared) schedule(static)
    
    """for i in range(nelr):
        for j in range(constVars['NVAR']): #for(int j = 0; j < NVAR; j++) 
            variables[i + j*nelr] = ff_variable[j];"""
    numIters = (nelr * constVars['numParts'] * constVars['NVAR'])

    # Assuming that the original layout of data is (ff_var[0]  repeated nelr times), (ff_var[1]repeated nelr times),(ff_var[2] repeated nelr times),(ff_var[3] repeated nelr times)
    # So, changing it to (ff_var[0] repeated (nelr'*num_parts) times), ... where nelr' = nelr/num_parts
    # Whenever variables is accessed 0 to nelr', the offset for ff_var[0], ff_var[1],.. should be nelr*num_parts
    # Whenever variable is accessed from 0 to nelr, the offest is the original nelr.
    for j in range(constVars['NVAR']):
        print ("\t j: %d ff_variable: %f "%(j,ff_variable[j]))

    for i in range(nelr*constVars['numParts']):
        #for j in range(constVars['numParts']):
        for j in range(constVars['NVAR']):
            #variables[i+(j+k)*nelr] = ff_variable[j]
            variables[i+(j*nelr*constVars['numParts'])] = ff_variable[j]

def compute_velocity(density,momentum,velocity):
    #(float& density, float3& momentum, float3& velocity)
    try: 
        velocity['x'] = momentum['x'] / density;
    except ZeroDivisionError:
        print ("\t density: %f ,momentum['x']: %f "%(density,momentum['x']))

    velocity['y'] = momentum['y'] / density;
    velocity['z'] = momentum['z'] / density;

def compute_speed_sqd(velocity):
    return velocity['x']*velocity['x'] + velocity['y']*velocity['y'] + velocity['z']*velocity['z'];

def compute_pressure(density,density_energy,speed_sqd):
    #(float& density, float& density_energy, float& speed_sqd):
    return (float(constVars['GAMMA'])-float(1.0))*(density_energy - float(0.5)*density*speed_sqd);

def compute_speed_of_sound(density,pressure):
    #(float& density, float& pressure)
    temp = float(constVars['GAMMA'])*pressure/density;
    if(temp<0):
        #print ("\t Gamma: %.4f pressure: %.4lf density: %.4lf temp: %.4lf "%(constVars['GAMMA'],pressure,density,temp))
        temp*=-1
    return math.sqrt(temp);

def compute_step_factor(nelr,variables,areas,step_factors):
    #compute_step_factor(int nelr, float* __restrict variables, float* areas, float* __restrict step_factors)
    for blk in range(int(nelr/constVars['block_length'])):
        b_start = int(blk*constVars['block_length']);
        b_end = nelr if ( (blk+1)*constVars['block_length'] > nelr) else int((blk+1)*constVars['block_length']);
        #print ("\t b_start: %s b_end: %s "%(b_start,b_end))
        for i in range(b_start,b_end): 
            prod = nelr*constVars['numParts'];
            # variables access range is from 0 to nelr'
            density = variables[i + constVars['VAR_DENSITY']*prod];
            momentum = {};
            momentum['x'] = variables[i + (constVars['VAR_MOMENTUM']+0)*prod];
            momentum['y'] = variables[i + (constVars['VAR_MOMENTUM']+1)*prod];
            momentum['z'] = variables[i + (constVars['VAR_MOMENTUM']+2)*prod];

            density_energy = variables[i + constVars['VAR_DENSITY_ENERGY']*prod];
            velocity  = {};
            compute_velocity(density, momentum, velocity);

            speed_sqd      = compute_speed_sqd(velocity);
            pressure       = compute_pressure(density, density_energy, speed_sqd);
            speed_of_sound = compute_speed_of_sound(density, pressure);

            #// dt = 0.5 * std::sqrt(areas[i]) /  (||v|| + c).... but when we do time stepping, this later would need to be divided by the area, so we just do it all at once
            #print ("\t nelr: %s areas[i]: %s speed_sqd: %s speed_of_sound: %s "%(nelr,areas[i],speed_sqd,speed_of_sound))
            prod = (math.sqrt(areas[i]) * (math.sqrt(speed_sqd) + speed_of_sound))
            if (prod==0):
                print ("\t i: %d math.sqrt(areas[i]): %.3f speed_of_sound: %.3f (math.sqrt(speed_sqd) + speed_of_sound): %.3f "%(i,math.sqrt(areas[i]),speed_of_sound,(math.sqrt(speed_sqd) + speed_of_sound)))
            step_factors[i] = float(0.5) / (math.sqrt(areas[i]) * (math.sqrt(speed_sqd) + speed_of_sound));

def time_step(j,nelr,old_variables,variables,step_factors,fluxes):
    #void time_step(int j, int nelr, float* old_variables, float* variables, float* step_factors, float* fluxes)
    for blk in range(int(nelr/constVars['block_length'])):
        b_start = int(blk*constVars['block_length']);
        b_end = nelr if ( (blk+1)*constVars['block_length'] > nelr) else int((blk+1)*constVars['block_length']);
        #print ("\t b_start: %s b_end: %s "%(b_start,b_end))
        for i in range(b_start,b_end): 
            factor = step_factors[i]/float(constVars['RK']+1-j);
            prod = nelr*constVars['numParts'];
            # variables access range is from 0 to nelr'
            variables[i + constVars['VAR_DENSITY']*prod] = old_variables[i + constVars['VAR_DENSITY']*prod] + factor*fluxes[i + constVars['VAR_DENSITY']*nelr];
            variables[i + (constVars['VAR_MOMENTUM']+0)*prod] = old_variables[i + (constVars['VAR_MOMENTUM']+0)*prod] + factor*fluxes[i + (constVars['VAR_MOMENTUM']+0)*nelr];
            variables[i + (constVars['VAR_MOMENTUM']+1)*prod] = old_variables[i + (constVars['VAR_MOMENTUM']+1)*prod] + factor*fluxes[i + (constVars['VAR_MOMENTUM']+1)*nelr];
            variables[i + (constVars['VAR_MOMENTUM']+2)*prod] = old_variables[i + (constVars['VAR_MOMENTUM']+2)*prod] + factor*fluxes[i + (constVars['VAR_MOMENTUM']+2)*nelr];
            variables[i + constVars['VAR_DENSITY_ENERGY']*prod] = old_variables[i + constVars['VAR_DENSITY_ENERGY']*prod] + factor*fluxes[i + constVars['VAR_DENSITY_ENERGY']*nelr];


def compute_flux_contribution(density,momentum,density_energy,pressure,velocity,fc_momentum_x,fc_momentum_y,fc_momentum_z,fc_density_energy):
    #(float& density, float3& momentum, float& density_energy, float& pressure, float3& velocity, float3& fc_momentum_x, float3& fc_momentum_y, 
    #float3& fc_momentum_z, float3& fc_density_energy)    
    fc_momentum_x['x'] = velocity['x']*momentum['x'] + pressure;
    fc_momentum_x['y'] = velocity['x']*momentum['y'];
    fc_momentum_x['z'] = velocity['x']*momentum['z'];

    fc_momentum_y['x'] = fc_momentum_x['y'];
    fc_momentum_y['y'] = velocity['y']*momentum['y'] + pressure;
    fc_momentum_y['z'] = velocity['y']*momentum['z'];

    fc_momentum_z['x'] = fc_momentum_x['z'];
    fc_momentum_z['y'] = fc_momentum_y['z'];
    fc_momentum_z['z'] = velocity['z']*momentum['z'] + pressure;

    de_p = density_energy+pressure;
    fc_density_energy['x'] = velocity['x']*de_p;
    fc_density_energy['y'] = velocity['y']*de_p;
    fc_density_energy['z'] = velocity['z']*de_p;

def compute_flux(nelr,elements_surrounding_elements,normals,variables,fluxes,ff_variable,ff_flux_contribution_momentum_x,ff_flux_contribution_momentum_y,ff_flux_contribution_momentum_z,ff_flux_contribution_density_energy):
    #compute_flux(int nelr, int* elements_surrounding_elements, float* normals, float* variables, float* fluxes, 
    #float* ff_variable, float3 ff_flux_contribution_momentum_x, float3 ff_flux_contribution_momentum_y, float3 ff_flux_contribution_momentum_z, 
    #float3 ff_flux_contribution_density_energy)   
    smoothing_coefficient = 0.2 
    for blk in range(int(nelr/constVars['block_length'])):
        b_start = int(blk*constVars['block_length']);
        b_end = nelr if ( (blk+1)*constVars['block_length'] > nelr) else int((blk+1)*constVars['block_length']);
        #print ("\t b_start: %s b_end: %s "%(b_start,b_end))
        for i in range(b_start,b_end): 
            prod = nelr*constVars['numParts']
            # variables access range is from 0 to nelr'
            density_i = variables[i + constVars['VAR_DENSITY']*prod];

            momentum_i = {};
            momentum_i['x'] = variables[i + (constVars['VAR_MOMENTUM']+0)*prod];
            momentum_i['y'] = variables[i + (constVars['VAR_MOMENTUM']+1)*prod];
            momentum_i['z'] = variables[i + (constVars['VAR_MOMENTUM']+2)*prod];

            density_energy_i = variables[i + constVars['VAR_DENSITY_ENERGY']*prod];
            velocity_i = {}
            compute_velocity(density_i, momentum_i, velocity_i);

            speed_sqd_i      = compute_speed_sqd(velocity_i);
            speed_i         = math.sqrt(speed_sqd_i) 
            pressure_i       = compute_pressure(density_i, density_energy_i, speed_sqd_i);
            speed_of_sound_i = compute_speed_of_sound(density_i, pressure_i);

            flux_contribution_i_momentum_x = {}; flux_contribution_i_momentum_y = {}; flux_contribution_i_momentum_z = {};flux_contribution_i_density_energy = {};
            compute_flux_contribution(density_i, momentum_i, density_energy_i, pressure_i, velocity_i, flux_contribution_i_momentum_x, flux_contribution_i_momentum_y, flux_contribution_i_momentum_z, flux_contribution_i_density_energy);

            flux_i_density = 0.0;
            flux_i_momentum = {};   
            flux_i_momentum['x'] = 0.0;
            flux_i_momentum['y'] = 0.0;
            flux_i_momentum['z'] = 0.0;
            flux_i_density_energy = 0.0;

            velocity_nb = {};momentum_nb = {};
            density_nb =0.0; density_energy_nb = 0.0;speed_sqd_nb = 0.0; speed_of_sound_nb = 0.0;  pressure_nb = 0.0;
            flux_contribution_nb_momentum_x = {}; flux_contribution_nb_momentum_y = {}; flux_contribution_nb_momentum_z = {};flux_contribution_nb_density_energy = {};

            for j in range(constVars['NNB']):
                normal = {}; 
                normal_len = 0.0; factor = 0.0;

                nb = int(elements_surrounding_elements[i + j*nelr]);
                normal['x'] = normals[i + (j + 0*constVars['NNB'])*nelr];
                normal['y'] = normals[i + (j + 1*constVars['NNB'])*nelr];
                normal['z'] = normals[i + (j + 2*constVars['NNB'])*nelr];
                normal_len = math.sqrt(normal['x']*normal['x'] + normal['y']*normal['y'] + normal['z']*normal['z']);

                if(nb >= 0):     
                    # Since nb can be anywhere from 0 to nelr and not 0 to nelr'
                    idx_z = nb + (constVars['VAR_MOMENTUM']+1)*nelr;
                    maxIdx = (nelr* constVars['NVAR'] * constVars['numParts'])
                    resIdx = nb + constVars['VAR_DENSITY_ENERGY']*prod
                    if(resIdx>=maxIdx): print ("\t nb: %s prod: %d constVars['VAR_DENSITY_ENERGY']*prod: %d resIdx: %d maxIdx: %s "%(nb,prod,constVars['VAR_DENSITY_ENERGY']*prod,resIdx,maxIdx))
                    density_nb       =     variables[nb + constVars['VAR_DENSITY']*prod];
                    momentum_nb['x'] =     variables[nb + (constVars['VAR_MOMENTUM']+0)*prod];
                    momentum_nb['y'] =     variables[nb + (constVars['VAR_MOMENTUM']+1)*prod];
                    momentum_nb['z'] =     variables[nb + (constVars['VAR_MOMENTUM']+2)*prod];
                    density_energy_nb = variables[nb + constVars['VAR_DENSITY_ENERGY']*prod];

                    compute_velocity(density_nb, momentum_nb, velocity_nb);
                    speed_sqd_nb                      = compute_speed_sqd(velocity_nb);
                    pressure_nb                       = compute_pressure(density_nb, density_energy_nb, speed_sqd_nb);
                    speed_of_sound_nb                 = compute_speed_of_sound(density_nb, pressure_nb);
                    compute_flux_contribution(density_nb, momentum_nb, density_energy_nb, pressure_nb, velocity_nb, flux_contribution_nb_momentum_x, flux_contribution_nb_momentum_y, flux_contribution_nb_momentum_z, flux_contribution_nb_density_energy);

                    # artificial viscosity
                    factor = -1*normal_len*smoothing_coefficient*0.5*(speed_i + math.sqrt(speed_sqd_nb) + speed_of_sound_i + speed_of_sound_nb);
                    flux_i_density += factor*(density_i-density_nb);
                    flux_i_density_energy += factor*(density_energy_i-density_energy_nb);
                    flux_i_momentum['x'] += factor*(momentum_i['x']-momentum_nb['x']);
                    flux_i_momentum['y'] += factor*(momentum_i['y']-momentum_nb['y']);
                    flux_i_momentum['z'] += factor*(momentum_i['z']-momentum_nb['z']);

                    # accumulate cell-centered fluxes
                    factor = 0.5*normal['x'];
                    flux_i_density += factor*(momentum_nb['x']+momentum_i['x']);
                    flux_i_density_energy += factor*(flux_contribution_nb_density_energy['x']+flux_contribution_i_density_energy['x']);
                    flux_i_momentum['x'] += factor*(flux_contribution_nb_momentum_x['x']+flux_contribution_i_momentum_x['x']);
                    flux_i_momentum['y'] += factor*(flux_contribution_nb_momentum_y['x']+flux_contribution_i_momentum_y['x']);
                    flux_i_momentum['z'] += factor*(flux_contribution_nb_momentum_z['x']+flux_contribution_i_momentum_z['x']);

                    factor = 0.5*normal['y'];
                    flux_i_density += factor*(momentum_nb['y']+momentum_i['y']);
                    flux_i_density_energy += factor*(flux_contribution_nb_density_energy['y']+flux_contribution_i_density_energy['y']);
                    flux_i_momentum['x'] += factor*(flux_contribution_nb_momentum_x['y']+flux_contribution_i_momentum_x['y']);
                    flux_i_momentum['y'] += factor*(flux_contribution_nb_momentum_y['y']+flux_contribution_i_momentum_y['y']);
                    flux_i_momentum['z'] += factor*(flux_contribution_nb_momentum_z['y']+flux_contribution_i_momentum_z['y']);

                    factor = 0.5*normal['z'];
                    flux_i_density += factor*(momentum_nb['z']+momentum_i['z']);
                    flux_i_density_energy += factor*(flux_contribution_nb_density_energy['z']+flux_contribution_i_density_energy['z']);
                    flux_i_momentum['x'] += factor*(flux_contribution_nb_momentum_x['z']+flux_contribution_i_momentum_x['z']);
                    flux_i_momentum['y'] += factor*(flux_contribution_nb_momentum_y['z']+flux_contribution_i_momentum_y['z']);
                    flux_i_momentum['z'] += factor*(flux_contribution_nb_momentum_z['z']+flux_contribution_i_momentum_z['z']);

                elif(nb == -1): # a wing boundary
                    flux_i_momentum['x'] += normal['x']*pressure_i;
                    flux_i_momentum['y'] += normal['y']*pressure_i;
                    flux_i_momentum['z'] += normal['z']*pressure_i;
                    
                elif(nb == -2): # a far field boundary
                    factor = 0.5*normal['x'];
                    flux_i_density += factor*(ff_variable[constVars['VAR_MOMENTUM']+0]+momentum_i['x']);
                    flux_i_density_energy += factor*(ff_flux_contribution_density_energy['x']+flux_contribution_i_density_energy['x']);
                    flux_i_momentum['x'] += factor*(ff_flux_contribution_momentum_x['x'] + flux_contribution_i_momentum_x['x']);
                    flux_i_momentum['y'] += factor*(ff_flux_contribution_momentum_y['x'] + flux_contribution_i_momentum_y['x']);
                    flux_i_momentum['z'] += factor*(ff_flux_contribution_momentum_z['x'] + flux_contribution_i_momentum_z['x']);

                    factor = 0.5*normal['y'];
                    flux_i_density += factor*(ff_variable[constVars['VAR_MOMENTUM']+1]+momentum_i['y']);
                    flux_i_density_energy += factor*(ff_flux_contribution_density_energy['y']+flux_contribution_i_density_energy['y']);
                    flux_i_momentum['x'] += factor*(ff_flux_contribution_momentum_x['y'] + flux_contribution_i_momentum_x['y']);
                    flux_i_momentum['y'] += factor*(ff_flux_contribution_momentum_y['y'] + flux_contribution_i_momentum_y['y']);
                    flux_i_momentum['z'] += factor*(ff_flux_contribution_momentum_z['y'] + flux_contribution_i_momentum_z['y']);

                    factor = 0.5*normal['z'];
                    flux_i_density += factor*(ff_variable[constVars['VAR_MOMENTUM']+2]+momentum_i['z']);
                    flux_i_density_energy += factor*(ff_flux_contribution_density_energy['z']+flux_contribution_i_density_energy['z']);
                    flux_i_momentum['x'] += factor*(ff_flux_contribution_momentum_x['z'] + flux_contribution_i_momentum_x['z']);
                    flux_i_momentum['y'] += factor*(ff_flux_contribution_momentum_y['z'] + flux_contribution_i_momentum_y['z']);
                    flux_i_momentum['z'] += factor*(ff_flux_contribution_momentum_z['z'] + flux_contribution_i_momentum_z['z']);

            fluxes[i + constVars['VAR_DENSITY']*nelr] = flux_i_density;
            fluxes[i + (constVars['VAR_MOMENTUM']+0)*nelr] = flux_i_momentum['x'];
            fluxes[i + (constVars['VAR_MOMENTUM']+1)*nelr] = flux_i_momentum['y'];
            fluxes[i + (constVars['VAR_MOMENTUM']+2)*nelr] = flux_i_momentum['z'];
            fluxes[i + constVars['VAR_DENSITY_ENERGY']*nelr] = flux_i_density_energy;

def getConn(allParams):

    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = hostname,
            port = portNum,
            is_secure=False,               # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    allParams['conn'] = conn
    return conn

def readObject(allParams,args):

    name = args.get("params", "stranger")
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        bucketName = msg["bucketName"] #args.get("bucketName","cfd_data") 
        keyName = msg["keyName"] #args.get("keyName","fvcorr.domn.193K") 
        numParts = msg["numParts"] #args.get("keyName","fvcorr.domn.193K")  
        keySuffix = msg["keySuffix"] #args.get("keyName","fvcorr.domn.193K")  
        curPartNum = msg["curPartNum"]
    else:
        return "Error: params not found!! name -->"+str(name)

    #bucketName = "cfd_data"
    #keyName = "fvcorDomn193K.p1"
    #constVars['numParts'] = 2 # hack, should be passed during invocation.

    conn = getConn(allParams)
    # Create bucket and an object
    try: 
        bucket = conn.get_bucket(bucketName)
    except boto.exception.S3ResponseError:
        return "Error: bucket missing "

    key = bucket.get_key(keyName)
    localPath = 'temp_'+str(keyName)+".log"
    try:
        key.get_contents_to_filename(localPath)    
    except AttributeError:
        return "Error: file/key missing "        

    allParams['path'] = localPath
    allParams['conn'] = conn
    allParams['bucketName'] = bucketName
    allParams['curPartNum'] = curPartNum
    allParams['keySuffix'] = keySuffix
    allParams['numParts'] = numParts
    allParams['exchangeNum'] = -1
    allParams["fluxKey"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])
    allParams["fluxFilename"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])+".log"
    return "Alright!"

def synchronizeFluxes(allParams):
    #push fluxes. 
    conn = allParams['conn'] # 
    #conn = getConn(allParams)
    try: 
        bucket = conn.get_bucket(allParams['bucketName'])
    except boto.exception.S3ResponseError:
        return "Error: bucket missing "

    #tempFilename = str(allParams['keySuffix'])+"_"str(allParams["curPartNum"])+".log"
    with open(allParams["fluxFilename"], 'w') as f:
        for item in allParams["fluxes"]:
            f.write("%s\n" % item)

    allParams['exchangeNum']+=1
    allParams["fluxKey"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])+"_"+str(allParams['exchangeNum'])
    myFluxKey = bucket.new_key(allParams["fluxKey"])
    myFluxKey.set_contents_from_filename(allParams["fluxFilename"])
    sizeFile = subprocess.check_output("ls -ltr "+str(allParams["fluxFilename"]),shell=True)
    print ("\t In sync str(allParams[keySuffix]): %s and sizeFile: --%s-- "%(str(allParams["keySuffix"]),sizeFile))
    # wait     
    startPartNum = 0; allFilesNotAccessed=True
    numFilesAccessed = 1; # I already have my file.
    listOfPartsFound = [allParams['curPartNum']]
    numAttempts = 0
    while allFilesNotAccessed:
        if(numFilesAccessed==allParams['numParts']):
            allFilesNotAccessed = False
        for curPartNum in range(startPartNum,constVars['numParts']):
            if(curPartNum in listOfPartsFound): continue;

            # doing this as a hack to just check the logic. Should test it on serverless-3.
            curFluxKeyname = str(allParams["keySuffix"])+"_"+str(curPartNum)+"_"+str(allParams['exchangeNum'])
            opFilename = str(allParams["keySuffix"])+"_"+str(curPartNum)+".log" #(doing "+1" as a hack for testing.)
            if(numAttempts%10==0): print ("\t curPartNum: %d allParams[fluxKey]: %s curFluxKeyname: %s "%(curPartNum,allParams["fluxKey"],curFluxKeyname))
            numAttempts+=1
            curFluxKey = bucket.get_key(curFluxKeyname)
            try:
                curFluxKey.get_contents_to_filename(opFilename)
                listOfPartsFound.append(curPartNum)
                #startPartNum+=1 # will only check for other cases, which haven't been accessed.
                numFilesAccessed+=1
                print ("\t Found flux file %s for key--> %s "%(opFilename,curFluxKeyname))
                if(numFilesAccessed==allParams['numParts']):
                    allFilesNotAccessed = False
                break ;
            except AttributeError:
                time.sleep(0.1) #  
                #break; # will poll on only one file.
                continue # can use this to ensure all the keys are accessed instead of polling on one of them.
        
    # if part0, remove all the flux from this iteration.
    print ("\t now I have finished receiving files from all of them! ")
    # Pending: should delete prev fluxes. 
    # Pending: calculating startIdx.
    for curPartNum in range(constVars['numParts']):
        startIdx = allParams['nelr']*curPartNum; #
        if(curPartNum!=allParams['curPartNum']):
            opFilename = str(allParams["keySuffix"])+"_"+str(curPartNum)+".log"
            readFluxesFile(allParams,opFilename,startIdx)
            subprocess.check_output("rm "+str(opFilename),shell=True) # So that we wont reread this in next iteration.           

        else: #this is my part, so copy inmemory ds.
            offset = [ constVars['VAR_DENSITY'],constVars['VAR_MOMENTUM'],constVars['VAR_MOMENTUM']+1,constVars['VAR_MOMENTUM']+2,constVars['VAR_DENSITY'] ]  
            for curVar in range(constVars['NVAR']):
                prod = allParams['nelr']*constVars['numParts']
                prodOffset_S = prod * offset[curVar];
                prodOffset_D = allParams['nelr'] * offset[curVar];
                # startIdx: offset corresponding to a part;
                # curIdx: offset related to diff elements within a part. 
                # prodOffset: offset corresponding to diff variable.
                for curIdx in range(allParams['nelr']):
                    allParams['sync_fluxes'][startIdx+curIdx+prodOffset_S] = allParams['fluxes'][curIdx+prodOffset_D]

    return

def deleteVarFiles(allParams):
    conn = allParams['conn'] # 
    #conn = getConn(allParams)
    try: 
        bucket = conn.get_bucket(allParams['bucketName'])
    except boto.exception.S3ResponseError:
        return "Error: bucket missing "

    for curExchangeNum in range(allParams['exchangeNum']):
        for curPartNum in range(constVars['numParts']):
            if(curPartNum==0): # Only part-0 will delete the objects so that there wont be any stray buckets from previous iterations
                curFluxKeyname = str(allParams["keySuffix"])+"_"+str(curPartNum)+"_"+str(curExchangeNum)
                bucket.delete_key(curFluxKeyname)
            # Everyone will delete the local file, so that there is no issue if same container is reused.     
    return 

def main(argv):
    allParams = {}
    #"""
    res = readObject(allParams,argv)
    if(res!="Alright!"):
        return {"greetings":"some issue with reading the file. ErrorMsg --> "+str(res)}
    constVars['numParts'] = allParams['numParts']

    # should remove when we uncomment the above part
    """
    allParams['path'] = "../componentExamples/load_files/nr1_fvcorr.domn.193K" #"part_1.log" #"/home/amoghli13/PACE/serverless/serverless-training-set/rodinia_cfd/componentExamples/nr_4_fvcorr.domn.193K"
    constVars['numParts'] = 1
    allParams['numParts'] = 1 # should come in as a parameter 
    allParams['bucketName'] = "cfd_data"
    allParams['keySuffix'] = "misc" #"cfd_np2_part"
    allParams['curPartNum'] = 0
    allParams['exchangeNum'] = 0
    constVars['numParts'] = allParams['numParts']
    allParams["fluxKey"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])
    allParams["fluxFilename"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])+".log"
    #"""
    # end of removal.
    path = allParams['path']
    print ("\t path: %s "%(path))
    initParams(allParams)
    #print ("\t allParams['ff_flux_contribution_momentum_x']: %s "%(allParams['ff_flux_contribution_momentum_x']))
    readFile(allParams['path'],allParams)
    
    # Solving part.. 
    # Create arrays and set initial conditions
    varSizes = (allParams['nelr'])*constVars['NVAR']*constVars['numParts'] # defensively increasing by 1 element.
    variables = [0.0 for x in range(varSizes)]
    #print ("\t allParams['nelr']*constVars['NVAR']: %d "%(allParams['nelr']*constVars['NVAR']))
    initialize_variables(allParams['nelr'], variables, allParams['ff_variable']);

    old_variables = [0.0 for x in range(varSizes)]
    fluxes = [0.0 for x in range(allParams['nelr']*constVars['NVAR'])]
    sync_fluxes = [0.0 for x in range(varSizes)]
    step_factors = [0.0 for x in range(allParams['nelr'])]

    allParams['variables'] = variables
    allParams['old_variables'] = old_variables
    allParams['fluxes'] = fluxes
    allParams['sync_fluxes'] = sync_fluxes
    allParams['step_factors'] = step_factors

    #print ("\t allParams['variables'][0:20]: %s "%(str(allParams['variables'][0:20])))
    begin = time.clock()

    # Begin iterations
    for i in range(constVars['iterations']):
        allParams['old_variables'] = [x for x in (allParams['variables'])]
        #copy<float>(old_variables, variables, nelr*NVAR);
        # print ("\t Before --> allParams['step_factors'][0:20]: %s "%(str(allParams['step_factors'][0:20])))
        # for the first iteration we compute the time step
        compute_step_factor(allParams['nelr'], allParams['variables'], allParams['areas'], allParams['step_factors']);
        # print ("\t After --> allParams['step_factors'][0:20]: %s "%(str(allParams['step_factors'][0:20])))

        #print ("\t Before --> allParams['fluxes'][0:20]: %s "%(str(allParams['fluxes'][0:20])))
        for j in range(constVars['RK']):
            compute_flux(allParams['nelr'],allParams['elements_surrounding_elements'],allParams['normals'],allParams['variables']
                ,allParams['fluxes'],allParams['ff_variable'],allParams['ff_flux_contribution_momentum_x'],allParams['ff_flux_contribution_momentum_y']
                ,allParams['ff_flux_contribution_momentum_z'],allParams['ff_flux_contribution_density_energy'])
            #(nelr, elements_surrounding_elements, normals, variables, fluxes, ff_variable, ff_flux_contribution_momentum_x, ff_flux_contribution_momentum_y,ff_flux_contribution_momentum_z, ff_flux_contribution_density_energy);

            # should synchronize
            synchronizeFluxes(allParams) # push local flux, wait for remote fluxes
            #print ("\t Hows it going?")
            #time_step(j, allParams['nelr'], allParams['old_variables'], allParams['variables'], allParams['step_factors'], allParams['fluxes']);
            time_step(j, allParams['nelr'], allParams['old_variables'], allParams['variables'], allParams['step_factors'], allParams['sync_fluxes']);

        #print ("\t After --> allParams['fluxes'][0:20]: %s "%(str(allParams['fluxes'][0:20])))
        end = time.clock();
        diff = end-begin
        #if(i%10==0): print ("\t i: %d diff: %.6lf "%(i,diff))
        break;
    print ("\t End: %s Begin: %s Time: %s "%(end,begin,diff))
    resultStr = "Took %.4lf time "%(diff)
    if(allParams["curPartNum"]==0):
        deleteVarFiles(allParams)
    return {"greetings" : resultStr}


if __name__ == "__main__":
    main(sys.argv)

